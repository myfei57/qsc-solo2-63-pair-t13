"""Priority recheck backlog with aging and cutoff protection.

The queue keeps FIFO as its default: ordinary bags are served in arrival order.
Two policies are layered on top instead of replacing it:

* **Expedite bands.** Bags sit in one of three bands (normal, priority,
  protected). Served order is band strictness first and arrival order inside a
  band, which is what keeps patient bags consistent with each other. An
  expedite may claim the whole front of the queue or only jump the plain
  queue; the choice is recorded together with every bag it overtakes.
* **Aging pass.** A normal bag waiting past the aging threshold is lifted once
  into the priority band, so a steady stream of expedites cannot starve it. A
  bag whose flight cutoff is inside the protection window is lifted once into
  the protected band, the floor an expedite is never allowed to cross, so rate
  limiting cannot push it past its deadline.

Every enqueue position, band change and reorder is journaled; priority changes
therefore replay identically and can be reviewed entry by entry against the
audit chain.
"""

from __future__ import annotations

from dataclasses import replace

from bagsort.clock import Clock, parse_iso
from bagsort.errors import ConflictError
from bagsort.model.entities import Bag, RecheckEntry
from bagsort.model.enums import BagState, ExpeditePolicy, PromotionCause, RecheckPriority
from bagsort.model.errors import unknown_bag
from bagsort.store.store import Store
from .policy import AgingPolicy
from .records import EnqueueResult, Promotion, QueueRow, ReorderResult

_PROMOTED_FLAGS = {
    PromotionCause.AGING: "aging_promoted",
    PromotionCause.CUTOFF: "cutoff_promoted",
}


class RecheckQueue:
    """Banded backlog of bags waiting for a second look."""

    def __init__(
        self,
        store: Store,
        book: object,
        clock: Clock,
        aging_policy: AgingPolicy | None = None,
        expedite_policy: ExpeditePolicy = ExpeditePolicy.HEAD,
    ) -> None:
        self._store = store
        self._book = book
        self._clock = clock
        self._aging = aging_policy or AgingPolicy()
        self._default_policy = expedite_policy

    # -- enqueue -----------------------------------------------------------

    def enqueue(
        self,
        bag: Bag,
        priority: RecheckPriority = RecheckPriority.NORMAL,
        policy: ExpeditePolicy | None = None,
    ) -> EnqueueResult:
        """Put a bag in the backlog; an enqueued bag is a no-op replay.

        ``priority`` selects the arrival band; ``policy`` only matters for the
        priority band and decides whether plain waiters or every waiter is
        overtaken.
        """

        state = self._store.state()
        existing = state.recheck_entries.get(bag.id)
        if existing is not None:
            return EnqueueResult(
                bag_id=bag.id,
                position=state.recheck_queue.index(bag.id),
                length=len(state.recheck_queue),
                priority=existing.priority,
                policy=policy or self._default_policy,
                starved=[],
            )

        updated = bag.evolved(state=BagState.RECHECKED)
        at = self._clock.now_iso()
        arrival_seq = state.recheck_arrival_seq + 1
        entry = RecheckEntry(
            bag_id=updated.id,
            arrival_seq=arrival_seq,
            enqueued_at=at,
            effective_at=at,
            priority=priority,
        )
        policy = policy or self._default_policy
        position, starved = self._insertion(state.recheck_queue, state.recheck_entries, priority, policy, updated.id)
        with self._store.session() as session:
            session.state.bags[updated.id] = updated
            session.state.recheck_entries[entry.bag_id] = entry
            session.state.recheck_arrival_seq = arrival_seq
            session.state.recheck_queue.insert(position, entry.bag_id)
            session.record("bag.put", {"bag": updated.as_dict()}, at)
            session.record(
                "recheck.enqueue",
                {"entry": entry.as_dict(), "position": position, "policy": policy.value},
                at,
            )
            session.record("counter.add", {"name": "recheck.enqueued", "delta": 1}, at)
        return EnqueueResult(
            bag_id=updated.id,
            position=position,
            length=len(self._store.state().recheck_queue),
            priority=priority,
            policy=policy,
            starved=starved,
        )

    def expedite(
        self, bag_id: str, policy: ExpeditePolicy | None = None
    ) -> EnqueueResult:
        """Mark a queued bag expedited and move it past patient waiters."""

        state = self._store.state()
        entry = self._require_entry(bag_id)
        policy = policy or self._default_policy
        at = self._clock.now_iso()
        if entry.priority is RecheckPriority.PROTECTED:
            # The cutoff floor must not be demoted by an expedite request.
            raise ConflictError(f"bag {bag_id!r} is protected by its cutoff window")

        order = [item for item in state.recheck_queue if item != bag_id]
        entries = state.recheck_entries
        position, starved = self._insertion(order, entries, RecheckPriority.PRIORITY, policy, bag_id)
        order.insert(position, bag_id)
        updated = entry.evolved(
            priority=RecheckPriority.PRIORITY, effective_at=at, last_promoted_at=at
        )
        with self._store.session() as session:
            session.state.recheck_entries[bag_id] = updated
            session.state.recheck_queue = list(order)
            session.record(
                "recheck.promote",
                {
                    "bag_id": bag_id,
                    "priority": RecheckPriority.PRIORITY.value,
                    "cause": PromotionCause.MANUAL.value,
                    "at": at,
                },
                at,
            )
            session.record("recheck.reorder", {"order": list(order)}, at)
            session.record("counter.add", {"name": "recheck.expedited", "delta": 1}, at)
        return EnqueueResult(
            bag_id=bag_id,
            position=position,
            length=len(order),
            priority=RecheckPriority.PRIORITY,
            policy=policy,
            starved=starved,
        )

    # -- aging / cutoff ----------------------------------------------------

    def reorder(self) -> ReorderResult:
        """Apply aging and cutoff protection, then stabilise the order.

        A bag is promoted at most once per cause: the ``aging_promoted`` and
        ``cutoff_promoted`` latches make a second automatic lift impossible.
        """

        state = self._store.state()
        at = self._clock.now_iso()
        now = self._clock.now()
        promotions: list[Promotion] = []
        updated: dict[str, RecheckEntry] = {}

        for bag_id in state.recheck_queue:
            entry = state.recheck_entries[bag_id]
            bag = state.bags.get(bag_id)
            waited = self._waited_seconds(entry, now)
            seconds_to_cutoff, cutoff_at = self._cutoff_window(bag, now)
            flight_id = bag.flight_id if bag is not None else ""

            if (
                not entry.cutoff_promoted
                and seconds_to_cutoff is not None
                and self._aging.cutoff_near(seconds_to_cutoff)
            ):
                target, cause = RecheckPriority.PROTECTED, PromotionCause.CUTOFF
            elif (
                entry.priority is RecheckPriority.NORMAL
                and not entry.aging_promoted
                and self._aging.aged_out(waited)
            ):
                # Lifting a normal waiter into the priority band is the aging
                # move. Manual expedites are operator decisions audited on
                # their own; they are not counted as an aging promotion and a
                # bag that reached a band is never re-lifted by aging.
                target, cause = RecheckPriority.PRIORITY, PromotionCause.AGING
            else:
                continue

            promoted_entry = replace(
                entry,
                priority=target,
                effective_at=at,
                last_promoted_at=at,
                **{_PROMOTED_FLAGS[cause]: True},
            )
            updated[bag_id] = promoted_entry
            promotions.append(
                Promotion(
                    bag_id=bag_id,
                    priority=target,
                    cause=cause,
                    at=at,
                    waited_seconds=waited,
                    seconds_to_cutoff=seconds_to_cutoff,
                    flight_id=flight_id,
                    cutoff_at=cutoff_at,
                )
            )

        entries = dict(state.recheck_entries)
        entries.update(updated)
        new_order = state.recheck_queue
        if promotions:
            # Promotion is the starvation fix: the affected bands are re-asserted
            # in arrival order, so head jumps that only landed after a patient
            # bag arrived can no longer stay ahead of it. Bags a jump legitimately
            # overtook at its time (older arrivals the policy named) keep their
            # relative place. Every positional change is reported in ``moved``.
            new_order = self._arrival_order(state.recheck_queue, entries)
        old_positions = {bag_id: index for index, bag_id in enumerate(state.recheck_queue)}
        moved = [
            bag_id
            for index, bag_id in enumerate(new_order)
            if old_positions.get(bag_id) != index
        ]

        if promotions or new_order != state.recheck_queue:
            with self._store.session() as session:
                for promotion in promotions:
                    session.state.recheck_entries[promotion.bag_id] = entries[promotion.bag_id]
                    session.record(
                        "recheck.promote",
                        {
                            "bag_id": promotion.bag_id,
                            "priority": promotion.priority.value,
                            "cause": promotion.cause.value,
                            "aging_promoted": promotion.cause is PromotionCause.AGING,
                            "cutoff_promoted": promotion.cause is PromotionCause.CUTOFF,
                            "at": at,
                        },
                        at,
                    )
                    counter = (
                        "recheck.cutoff_protected"
                        if promotion.cause is PromotionCause.CUTOFF
                        else "recheck.aging_promoted"
                    )
                    session.record("counter.add", {"name": counter, "delta": 1}, at)
                session.state.recheck_queue = list(new_order)
                session.record("recheck.reorder", {"order": list(new_order)}, at)
        return ReorderResult(at=at, order=list(new_order), promotions=promotions, moved=moved)

    def promote(
        self,
        bag_id: str,
        priority: RecheckPriority,
        cause: PromotionCause = PromotionCause.MANUAL,
    ) -> Promotion:
        """Operator driven band change; every call is audited on its own."""

        entry = self._require_entry(bag_id)
        state = self._store.state()
        at = self._clock.now_iso()
        now = self._clock.now()
        bag = state.bags.get(bag_id)
        waited = self._waited_seconds(entry, now)
        seconds_to_cutoff, cutoff_at = self._cutoff_window(bag, now)
        flight_id = bag.flight_id if bag is not None else ""

        flag = _PROMOTED_FLAGS.get(cause)
        changes: dict[str, object] = {
            "priority": priority,
            "effective_at": at,
            "last_promoted_at": at,
        }
        if flag is not None and not getattr(entry, flag):
            changes[flag] = True
        updated = replace(entry, **changes)
        entries = dict(state.recheck_entries)
        entries[bag_id] = updated
        new_order = [item for item in state.recheck_queue if item != bag_id]
        self._insert_promoted(new_order, entries, bag_id)

        with self._store.session() as session:
            session.state.recheck_entries[bag_id] = updated
            session.state.recheck_queue = list(new_order)
            session.record(
                "recheck.promote",
                {
                    "bag_id": bag_id,
                    "priority": priority.value,
                    "cause": cause.value,
                    "aging_promoted": cause is PromotionCause.AGING,
                    "cutoff_promoted": cause is PromotionCause.CUTOFF,
                    "at": at,
                },
                at,
            )
            session.record("recheck.reorder", {"order": list(new_order)}, at)
            session.record("counter.add", {"name": "recheck.manual_promoted", "delta": 1}, at)
        return Promotion(
            bag_id=bag_id,
            priority=priority,
            cause=cause,
            at=at,
            waited_seconds=waited,
            seconds_to_cutoff=seconds_to_cutoff,
            flight_id=flight_id,
            cutoff_at=cutoff_at,
        )

    # -- processing --------------------------------------------------------

    def dequeue(self) -> Bag | None:
        state = self._store.state()
        if not state.recheck_queue:
            return None
        bag_id = state.recheck_queue[0]
        bag = state.bags.get(bag_id)
        if bag is None:
            raise unknown_bag(bag_id)
        at = self._clock.now_iso()
        with self._store.session() as session:
            session.state.recheck_queue = [
                item for item in session.state.recheck_queue if item != bag_id
            ]
            session.state.recheck_entries.pop(bag_id, None)
            session.record("recheck.dequeue", {"bag_id": bag_id}, at)
            session.record("counter.add", {"name": "recheck.processed", "delta": 1}, at)
        return bag

    def peek(self) -> Bag | None:
        state = self._store.state()
        if not state.recheck_queue:
            return None
        return state.bags.get(state.recheck_queue[0])

    def items(self) -> list[Bag]:
        state = self._store.state()
        return [state.bags[bag_id] for bag_id in state.recheck_queue if bag_id in state.bags]

    def length(self) -> int:
        return len(self._store.state().recheck_queue)

    def clear(self) -> int:
        dropped = len(self._store.state().recheck_queue)
        with self._store.session() as session:
            session.state.recheck_queue = []
            session.state.recheck_entries = {}
            session.record("recheck.clear", {}, self._clock.now_iso())
        return dropped

    def deadline(self, flight_id: str) -> str:
        return self._book.deadline(flight_id)

    # -- cross checking ----------------------------------------------------

    def entries(self) -> list[RecheckEntry]:
        """Queue entries in served order, band and promotion latches included."""

        state = self._store.state()
        return [
            state.recheck_entries[bag_id]
            for bag_id in state.recheck_queue
            if bag_id in state.recheck_entries
        ]

    def entry(self, bag_id: str) -> RecheckEntry:
        return self._require_entry(bag_id)

    def view(self) -> list[QueueRow]:
        """Queue order joined with each bag's flight cutoff."""

        state = self._store.state()
        now = self._clock.now()
        rows: list[QueueRow] = []
        for position, bag_id in enumerate(state.recheck_queue):
            entry = state.recheck_entries.get(bag_id)
            if entry is None:
                continue
            bag = state.bags.get(bag_id)
            seconds_to_cutoff, cutoff_at = self._cutoff_window(bag, now)
            rows.append(
                QueueRow(
                    position=position,
                    bag_id=bag_id,
                    flight_id=bag.flight_id if bag is not None else "",
                    priority=entry.priority,
                    enqueued_at=entry.enqueued_at,
                    waited_seconds=self._waited_seconds(entry, now),
                    cutoff_at=cutoff_at,
                    seconds_to_cutoff=seconds_to_cutoff,
                    aging_promoted=entry.aging_promoted,
                    cutoff_promoted=entry.cutoff_promoted,
                )
            )
        return rows

    def promotion_history(self) -> list[dict[str, object]]:
        """Every committed band change, oldest first, for item by item review."""

        history: list[dict[str, object]] = []
        for journal_entry in self._store.entries():
            if journal_entry.kind == "recheck.promote":
                history.append(
                    {
                        "sequence": journal_entry.sequence,
                        "at": journal_entry.at,
                        **dict(journal_entry.payload),
                    }
                )
        return history

    # -- internals ---------------------------------------------------------

    def _require_entry(self, bag_id: str) -> RecheckEntry:
        entry = self._store.state().recheck_entries.get(bag_id)
        if entry is None:
            raise unknown_bag(bag_id)
        return entry

    def _insertion(
        self,
        order: list[str],
        entries: dict[str, RecheckEntry],
        priority: RecheckPriority,
        policy: ExpeditePolicy,
        bag_id: str,
    ) -> tuple[int, list[str]]:
        """Position for a newcomer/reinsertion and the bags it overtakes."""

        if priority is RecheckPriority.NORMAL:
            position = len(order)
        elif priority is RecheckPriority.PROTECTED:
            # Never cross another protected bag; everything else is overtaken.
            position = next(
                (
                    index
                    for index, other_id in enumerate(order)
                    if entries[other_id].priority is not RecheckPriority.PROTECTED
                ),
                len(order),
            )
        elif policy is ExpeditePolicy.AHEAD_OF_NORMAL:
            # Overtake plain waiters only; protected and already-promoted
            # priority bags keep their place ahead of the newcomer.
            position = next(
                (
                    index
                    for index, other_id in enumerate(order)
                    if entries[other_id].priority is RecheckPriority.NORMAL
                ),
                len(order),
            )
        else:  # PRIORITY + HEAD: right after the protected floor
            position = 0
            for index, other_id in enumerate(order):
                if entries[other_id].priority is RecheckPriority.PROTECTED:
                    position = index + 1
                else:
                    break
        starved = [other_id for other_id in order[position:] if other_id != bag_id]
        return position, starved

    def _arrival_order(
        self, order: list[str], entries: dict[str, RecheckEntry]
    ) -> list[str]:
        """Bands strict first; arrival order inside each band."""

        queued = [bag_id for bag_id in order if bag_id in entries]
        queued.extend(bag_id for bag_id in entries if bag_id not in queued)
        return sorted(queued, key=lambda bag_id: (entries[bag_id].priority.rank,) + self._arrival_key(entries, bag_id))

    def _arrival_key(self, entries: dict[str, RecheckEntry], bag_id: str) -> tuple[str, int]:
        entry = entries[bag_id]
        return (entry.enqueued_at, entry.arrival_seq)

    def _insert_promoted(
        self, order: list[str], entries: dict[str, RecheckEntry], bag_id: str
    ) -> None:
        """Place a freshly promoted bag into its target band in place.

        It lands at the end of that band's block but ahead of every bag in the
        same band that only arrived after it, so aging cannot be overtaken by
        later head jumps. Bags the promotion does not touch keep their relative
        positions.
        """

        promoted = entries[bag_id]
        arrival = self._arrival_key(entries, bag_id)
        position = len(order)
        for index, other_id in enumerate(order):
            other = entries[other_id]
            if other.priority.rank > promoted.priority.rank:
                # First position inside the target band block.
                position = index
                break
            if (
                other.priority is promoted.priority
                and self._arrival_key(entries, other_id) > arrival
            ):
                # A same-band bag that arrived later yields its place.
                position = index
                break
        order.insert(position, bag_id)

    def _waited_seconds(self, entry: RecheckEntry, now) -> float:
        enqueued = parse_iso(entry.enqueued_at)
        if enqueued is None:
            return 0.0
        return max(0.0, (now - enqueued).total_seconds())

    def _cutoff_window(self, bag: Bag | None, now) -> tuple[float | None, str]:
        if bag is None or not bag.flight_id:
            return None, ""
        try:
            cutoff_at = self._book.deadline(bag.flight_id)
        except Exception:
            return None, ""
        cutoff = parse_iso(cutoff_at)
        if cutoff is None:
            return None, ""
        return (cutoff - now).total_seconds(), cutoff_at
