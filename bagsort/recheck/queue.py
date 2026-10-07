"""Prioritised recheck backlog with ageing, cutoff protection and full auditing.

The committed queue is no longer a plain FIFO list. For every waiting bag the
store keeps a :class:`~bagsort.model.entities.RecheckEntry`; the exposed order
is always the pure projection in :mod:`bagsort.model.reorder`, so restart by
replay rebuilds the exact same arrangement.

Three things move a bag up, and each leaves its own audit entry:

* **Operator priority.** A bag may be marked urgent. ``before_normal`` urgents
  only overtake ordinary waiters and share one arrival ordered line with age
  promoted bags, so endless urgent traffic cannot starve the honest queue.
  ``head`` urgents also overtake that elevated line; the call records exactly
  which waiters were pushed back.
* **Ageing.** An ordinary bag waiting past the policy threshold is lifted once,
  and once only, into the elevated class, ahead of later arrivals but behind
  earlier ones -- FIFO consistency inside a class is preserved.
* **Cutoff guard.** A bag whose flight cutoff is at or inside the guard window
  is lifted to the highest class. This outranks every manual choice and is the
  backstop that stops rate limiting from pushing near cutoff baggage back
  without bound.
"""

from __future__ import annotations

from dataclasses import dataclass

from bagsort.clock import Clock
from bagsort.errors import ConflictError, ValidationError
from bagsort.model.entities import Bag, RecheckEntry, RecheckPolicy
from bagsort.model.enums import BagState, RecheckPriority, UrgentPlacement
from bagsort.model.errors import unknown_bag
from bagsort.model.reorder import (
    cutoff_deadline,
    entry_class,
    near_cutoff,
    ordered_entries,
    ordering_ok,
    parse_iso,
    promotion_plan,
)
from bagsort.store.store import Store


@dataclass(frozen=True)
class QueueMove:
    """One bag's promotion, with the queues immediately around it for review."""

    bag_id: str
    reason: str
    from_class: str
    to_class: str
    cutoff_at: str
    displaced: tuple[dict[str, object], ...]


class RecheckQueue:
    """Priority backlog of bags waiting for a second look."""

    def __init__(self, store: Store, book: object, clock: Clock) -> None:
        self._store = store
        self._book = book
        self._clock = clock

    # -- reading -----------------------------------------------------------

    def entries(self) -> list[RecheckEntry]:
        state = self._store.state()
        return [state.recheck_entries[bag_id] for bag_id in state.recheck_queue]

    def entry(self, bag_id: str) -> RecheckEntry:
        entry = self._store.state().recheck_entries.get(bag_id)
        if entry is None:
            raise ConflictError(f"bag {bag_id!r} is not in the recheck queue")
        return entry

    def items(self) -> list[Bag]:
        state = self._store.state()
        return [state.bags[bag_id] for bag_id in state.recheck_queue if bag_id in state.bags]

    def peek(self) -> Bag | None:
        state = self._store.state()
        if not state.recheck_queue:
            return None
        return state.bags.get(state.recheck_queue[0])

    def length(self) -> int:
        return len(self._store.state().recheck_queue)

    def policy(self) -> RecheckPolicy:
        return self._store.state().recheck_policy

    def order(self) -> list[dict[str, object]]:
        """The processing order joined with flight cutoff and class detail."""

        state = self._store.state()
        now = self._clock.now()
        rows: list[dict[str, object]] = []
        for position, entry in enumerate(
            state.recheck_entries[bag_id] for bag_id in state.recheck_queue
        ):
            deadline = cutoff_deadline(state.flights.get(entry.flight_id))
            cutoff = parse_iso(deadline)
            rows.append(
                {
                    **entry.as_dict(),
                    "position": position,
                    "class": entry_class(entry),
                    "deadline": deadline,
                    "seconds_to_cutoff": (
                        int((cutoff - now).total_seconds()) if cutoff is not None else None
                    ),
                }
            )
        return rows

    def deadline(self, flight_id: str) -> str:
        return self._book.deadline(flight_id)

    # -- mutation ----------------------------------------------------------

    def enqueue(
        self,
        bag: Bag,
        priority: RecheckPriority = RecheckPriority.NORMAL,
        placement: UrgentPlacement | None = None,
    ) -> dict[str, object]:
        updated = bag.evolved(state=BagState.RECHECKED)
        at = self._clock.now_iso()
        prior = self._store.state()
        if updated.id in prior.recheck_entries:
            existing = prior.recheck_entries[updated.id]
            return {
                "bag_id": updated.id,
                "idempotent": True,
                "priority": existing.priority.value,
                "class": entry_class(existing),
                "placed": False,
                "promotions": [],
                "before": list(prior.recheck_queue),
                "after": list(prior.recheck_queue),
                "displaced": [],
            }

        policy = prior.recheck_policy
        before = list(prior.recheck_queue)
        entry = RecheckEntry(
            bag_id=updated.id,
            flight_id=updated.flight_id,
            arrival_at=at,
            arrival_seq=prior.recheck_seq + 1,
            priority=priority,
            placement=placement or policy.urgent_placement,
        )
        with self._store.session() as session:
            session.state.bags[updated.id] = updated
            session.state.recheck_seq = entry.arrival_seq
            session.state.recheck_entries[entry.bag_id] = entry
            session.record("bag.put", {"bag": updated.as_dict()}, at)
            session.record("recheck.enqueue", {"entry": entry.as_dict()}, at)
            session.record("counter.add", {"name": "recheck.enqueued", "delta": 1}, at)
            # cutoff protection is the only automation an arrival triggers:
            # a near cutoff newcomer must lead immediately, but ageing stays an
            # explicit rebalance decision instead of firing on late arrivals
            moves = self._promote(session, at, reasons=("cutoff",))
        after = list(self._store.state().recheck_queue)
        final = self._store.state().recheck_entries[updated.id]
        return {
            "bag_id": updated.id,
            "idempotent": False,
            "priority": final.priority.value,
            "class": entry_class(final),
            "placed": True,
            "promotions": moves,
            "before": before,
            "after": after,
            "displaced": _displaced(before, after),
        }

    def set_priority(
        self,
        bag_id: str,
        priority: RecheckPriority,
        placement: UrgentPlacement | None = None,
    ) -> dict[str, object]:
        """Change one bag's urgency; the response names every bag it starves."""

        state = self._store.state()
        current = state.recheck_entries.get(bag_id)
        if current is None:
            if bag_id not in state.bags:
                raise unknown_bag(bag_id)
            raise ConflictError(f"bag {bag_id!r} is not in the recheck queue")
        placement = placement or state.recheck_policy.urgent_placement
        if current.priority is priority and current.placement is placement:
            return {
                "bag_id": bag_id,
                "changed": False,
                "from_class": entry_class(current),
                "to_class": entry_class(current),
                "promotions": [],
                "before": list(state.recheck_queue),
                "after": list(state.recheck_queue),
                "displaced": [],
            }

        at = self._clock.now_iso()
        before = list(state.recheck_queue)
        from_class = entry_class(current)
        with self._store.session() as session:
            updated = current.evolved(priority=priority, placement=placement)
            session.state.recheck_entries[bag_id] = updated
            session.record(
                "recheck.priority",
                {"bag_id": bag_id, "priority": priority.value, "placement": placement.value},
                at,
            )
            session.record("counter.add", {"name": "recheck.priority_changes", "delta": 1}, at)
            moves = self._promote(session, at)
        final_state = self._store.state()
        after = list(final_state.recheck_queue)
        return {
            "bag_id": bag_id,
            "changed": True,
            "from_class": from_class,
            "to_class": entry_class(final_state.recheck_entries[bag_id]),
            "promotions": moves,
            "before": before,
            "after": after,
            "displaced": _displaced(before, after),
        }

    def rebalance(self) -> dict[str, object]:
        """Apply every ageing and cutoff promotion currently due."""

        state = self._store.state()
        before = list(state.recheck_queue)
        at = self._clock.now_iso()
        plan = promotion_plan(
            state.recheck_entries.values(),
            state.flights,
            state.recheck_policy,
            self._clock.now(),
        )
        if not plan:
            return {"changed": False, "promotions": [], "before": before, "after": before,
                    "displaced": []}
        with self._store.session() as session:
            moves = self._promote(session, at, plan=plan)
        after = list(self._store.state().recheck_queue)
        return {
            "changed": True,
            "promotions": moves,
            "before": before,
            "after": after,
            "displaced": _displaced(before, after),
        }

    def dequeue_detailed(self) -> tuple[Bag | None, list[QueueMove]]:
        state = self._store.state()
        if not state.recheck_queue:
            return None, []
        bag_id = state.recheck_queue[0]
        bag = state.bags.get(bag_id)
        if bag is None:
            raise unknown_bag(bag_id)
        at = self._clock.now_iso()
        with self._store.session() as session:
            session.state.recheck_entries.pop(bag_id, None)
            session.record("recheck.dequeue", {"bag_id": bag_id}, at)
            session.record("counter.add", {"name": "recheck.processed", "delta": 1}, at)
            moves = self._promote(session, at)
        return self._store.state().bags.get(bag_id, bag), moves

    def dequeue(self) -> Bag | None:
        bag, _moves = self.dequeue_detailed()
        return bag

    def clear(self) -> int:
        dropped = len(self._store.state().recheck_queue)
        with self._store.session() as session:
            session.state.recheck_entries = {}
            session.state.recheck_queue = []
            session.record("recheck.clear", {}, self._clock.now_iso())
        return dropped

    def set_policy(
        self,
        urgent_placement: UrgentPlacement | None = None,
        ageing_after_seconds: int | None = None,
        cutoff_lead_seconds: int | None = None,
    ) -> RecheckPolicy:
        current = self._store.state().recheck_policy
        updated = RecheckPolicy(
            urgent_placement=current.urgent_placement
            if urgent_placement is None
            else urgent_placement,
            ageing_after_seconds=current.ageing_after_seconds
            if ageing_after_seconds is None
            else ageing_after_seconds,
            cutoff_lead_seconds=current.cutoff_lead_seconds
            if cutoff_lead_seconds is None
            else cutoff_lead_seconds,
        )
        if updated.ageing_after_seconds < 0 or updated.cutoff_lead_seconds < 0:
            raise ValidationError("recheck policy windows must not be negative")
        if updated == current:
            return current
        with self._store.session() as session:
            session.state.recheck_policy = updated
            session.record("recheck.policy", {"policy": updated.as_dict()}, self._clock.now_iso())
        return updated

    # -- cross check --------------------------------------------------------

    def crosscheck(self, audit: object | None = None) -> dict[str, object]:
        """Join queue order, flight cutoffs and the audit trail for review."""

        state = self._store.state()
        issues: list[dict[str, object]] = []
        expected = [entry.bag_id for entry in ordered_entries(state.recheck_entries.values())]
        if list(state.recheck_queue) != expected:
            issues.append({"code": "order_drift", "expected": expected,
                           "committed": list(state.recheck_queue)})

        age_commands: dict[str, int] = {}
        guard_commands: dict[str, int] = {}
        priority_commands: dict[str, int] = {}
        for journal in self._store.entries():
            if journal.kind == "recheck.age":
                bag_id = str(journal.payload.get("bag_id", ""))
                age_commands[bag_id] = age_commands.get(bag_id, 0) + 1
            elif journal.kind == "recheck.guard":
                bag_id = str(journal.payload.get("bag_id", ""))
                guard_commands[bag_id] = guard_commands.get(bag_id, 0) + 1
            elif journal.kind == "recheck.priority":
                bag_id = str(journal.payload.get("bag_id", ""))
                priority_commands[bag_id] = priority_commands.get(bag_id, 0) + 1
        for bag_id, count in age_commands.items():
            if count > 1:
                issues.append({"code": "double_age_promotion", "bag_id": bag_id, "count": count})
        for bag_id, count in guard_commands.items():
            if count > 1:
                issues.append({"code": "double_cutoff_guard", "bag_id": bag_id, "count": count})

        policy = state.recheck_policy
        now = self._clock.now()
        for entry in (state.recheck_entries[bid] for bid in state.recheck_queue):
            if entry.bag_id not in state.bags:
                issues.append({"code": "missing_bag", "bag_id": entry.bag_id})
            deadline = cutoff_deadline(state.flights.get(entry.flight_id))
            if entry.cutoff_guarded and not deadline:
                issues.append({"code": "guard_without_cutoff", "bag_id": entry.bag_id})
            if (
                not entry.cutoff_guarded
                and near_cutoff(entry, deadline, now, policy.cutoff_lead_seconds)
            ):
                issues.append({"code": "cutoff_unprotected", "bag_id": entry.bag_id,
                               "deadline": deadline})

        if audit is not None:
            entries = audit.entries()
            kind_counts: dict[tuple[str, str], int] = {}
            for record in entries:
                kind_counts[(record.kind, record.subject)] = (
                    kind_counts.get((record.kind, record.subject), 0) + 1
                )

            def _audited(kind: str, bag_id: str) -> int:
                return kind_counts.get((kind, bag_id), 0)

            for bag_id, count in age_commands.items():
                if _audited("recheck_ageing", bag_id) < count:
                    issues.append({"code": "audit_gap", "bag_id": bag_id, "kind": "recheck_ageing"})
            for bag_id, count in guard_commands.items():
                if _audited("recheck_cutoff", bag_id) < count:
                    issues.append({"code": "audit_gap", "bag_id": bag_id, "kind": "recheck_cutoff"})
            for bag_id, count in priority_commands.items():
                if _audited("recheck_priority", bag_id) < count:
                    issues.append({"code": "audit_gap", "bag_id": bag_id,
                                   "kind": "recheck_priority"})
            for bag_id in state.recheck_entries:
                if _audited("recheck", bag_id) == 0:
                    issues.append({"code": "audit_gap", "bag_id": bag_id, "kind": "recheck"})

        return {
            "ok": not issues and ordering_ok(self.entries()),
            "issues": issues,
            "queue": list(state.recheck_queue),
            "order": self.order(),
            "policy": policy.as_dict(),
            "audit_intact": bool(audit.verify()) if audit is not None else None,
        }

    # -- internals ----------------------------------------------------------

    def _promote(
        self,
        session,
        at: str,
        plan: dict[str, str] | None = None,
        reasons: tuple[str, ...] = ("ageing", "cutoff"),
    ) -> list[QueueMove]:
        """Apply due promotions onto a buffered session and journal them.

        ``reasons`` limits which automation runs: enqueue only applies cutoff
        protection (ageing must not fire merely because a bag arrived late in
        wall time), while an explicit rebalance or a dequeue applies both.
        """

        state = session.state
        if plan is None:
            plan = promotion_plan(
                state.recheck_entries.values(),
                state.flights,
                state.recheck_policy,
                self._clock.now(),
            )
        before = [entry.bag_id for entry in ordered_entries(state.recheck_entries.values())]
        moves: list[QueueMove] = []
        for bag_id, reason in sorted(plan.items(), key=lambda item: item[0]):
            if reason not in reasons:
                continue
            entry = state.recheck_entries.get(bag_id)
            if entry is None:
                continue
            if reason == "cutoff" and not entry.cutoff_guarded:
                deadline = cutoff_deadline(state.flights.get(entry.flight_id))
                from_class = entry_class(entry)
                updated = entry.evolved(
                    cutoff_guarded=True, guarded_at=at, cutoff_at=deadline
                )
                state.recheck_entries[bag_id] = updated
                self._journal_promotion(
                    session,
                    "recheck.guard",
                    bag_id,
                    "recheck.cutoff_guarded",
                    {"bag_id": bag_id, "cutoff_at": deadline, "at": at},
                    at,
                )
                interim = [item.bag_id for item in ordered_entries(state.recheck_entries.values())]
                moves.append(
                    QueueMove(bag_id, "cutoff", from_class, entry_class(updated), deadline,
                              tuple(_displaced(before, interim)))
                )
                before = interim
            elif reason == "ageing" and not entry.age_promoted:
                from_class = entry_class(entry)
                updated = entry.evolved(age_promoted=True, aged_at=at)
                state.recheck_entries[bag_id] = updated
                self._journal_promotion(
                    session,
                    "recheck.age",
                    bag_id,
                    "recheck.age_promoted",
                    {"bag_id": bag_id, "at": at},
                    at,
                )
                interim = [item.bag_id for item in ordered_entries(state.recheck_entries.values())]
                moves.append(
                    QueueMove(bag_id, "ageing", from_class, entry_class(updated), "",
                              tuple(_displaced(before, interim)))
                )
                before = interim
        state.recheck_queue = [
            entry.bag_id for entry in ordered_entries(state.recheck_entries.values())
        ]
        return moves

    @staticmethod
    def _journal_promotion(
        session, kind: str, bag_id: str, counter: str, payload: dict[str, object], at: str
    ) -> None:
        session.record(kind, payload, at)
        session.record("counter.add", {"name": counter, "delta": 1}, at)


def _displaced(before: list[str], after: list[str]) -> list[dict[str, object]]:
    """Bags pushed strictly farther from the front by a rearrangement."""

    positions = {bag_id: index for index, bag_id in enumerate(before)}
    return [
        {"bag_id": bag_id, "from": positions[bag_id], "to": index}
        for index, bag_id in enumerate(after)
        if bag_id in positions and index > positions[bag_id]
    ]
