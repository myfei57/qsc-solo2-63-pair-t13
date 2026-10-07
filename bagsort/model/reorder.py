"""Pure ordering rules for the prioritised recheck backlog.

Nothing here touches the store or the clock: given the committed entries, the
flight cutoffs and a "now" instant, these functions decide the processing
order and which waiters qualify for an automatic promotion. Keeping the rules
pure is what lets the journal reducer rebuild the exact same order on restart
and lets an auditor recompute every past rearrangement offline.

Class order, highest first:

1. ``cutoff``  -- near cutoff guard. Nothing in the queue may push a guarded
   bag back without bound, so this class outranks every manual choice.
2. ``head_urgent`` -- urgent arrivals inserted at the head of the queue.
3. ``elevated`` -- urgent arrivals inserted only before ordinary waiters and
   bags the ager has lifted; together they keep one FIFO line, which is what
   stops a steady stream of urgent traffic from starving the honest waiters.
4. ``normal`` -- ordinary FIFO waiters.

Within a class the arrival sequence is the tie breaker: most recent first only
for ``head_urgent`` (that policy explicitly overtakes earlier urgents), arrival
first everywhere else, so equal priority is always arrival consistent.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import TYPE_CHECKING, Iterable

from .entities import RecheckEntry, RecheckPolicy
from .enums import RecheckPriority, UrgentPlacement

if TYPE_CHECKING:  # pragma: no cover - typing only
    from .entities import Flight

CUTOFF = "cutoff"
HEAD_URGENT = "head_urgent"
ELEVATED = "elevated"
NORMAL = "normal"

_CLASS_RANK = {CUTOFF: 0, HEAD_URGENT: 1, ELEVATED: 2, NORMAL: 3}


def parse_iso(value: str) -> datetime | None:
    """Parse the service's ``YYYY-MM-DDTHH:MM:SSZ`` stamps; empty is unknown."""

    if not value:
        return None
    text = value.strip()
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def entry_class(entry: RecheckEntry) -> str:
    """The ordering class an entry currently belongs to."""

    if entry.cutoff_guarded:
        return CUTOFF
    if entry.priority is RecheckPriority.URGENT and entry.placement is UrgentPlacement.HEAD:
        return HEAD_URGENT
    if entry.priority is RecheckPriority.URGENT or entry.age_promoted:
        return ELEVATED
    return NORMAL


def _sort_key(entry: RecheckEntry) -> tuple[int, int]:
    cls = entry_class(entry)
    if cls is HEAD_URGENT:
        # Latest head insertion is served first; negatives keep ascending sort.
        return _CLASS_RANK[cls], -entry.arrival_seq
    return _CLASS_RANK[cls], entry.arrival_seq


def ordered_entries(entries: Iterable[RecheckEntry]) -> list[RecheckEntry]:
    """Stable processing order derived purely from the entries' own fields."""

    return sorted(entries, key=_sort_key)


def ordering_ok(entries: Iterable[RecheckEntry]) -> bool:
    """Whether the given sequence already matches :func:`ordered_entries`."""

    entries = list(entries)
    return [entry.bag_id for entry in entries] == [
        entry.bag_id for entry in ordered_entries(entries)
    ]


def cutoff_deadline(flight: "Flight | None") -> str:
    return flight.cutoff_at if flight is not None else ""


def near_cutoff(
    entry: RecheckEntry, deadline: str, now: datetime, lead_seconds: int
) -> bool:
    """Whether a bag's cutoff is at or inside the guard window (or already due)."""

    cutoff = parse_iso(deadline)
    if cutoff is None:
        return False
    seconds_left = (cutoff - now).total_seconds()
    return seconds_left <= lead_seconds


def over_age(entry: RecheckEntry, now: datetime, threshold_seconds: int) -> bool:
    """Whether an ordinary, never promoted bag has waited beyond the age limit."""

    if entry.age_promoted or entry.cutoff_guarded:
        return False
    if entry.priority is RecheckPriority.URGENT:
        return False
    arrived = parse_iso(entry.arrival_at)
    if arrived is None:
        return False
    return (now - arrived).total_seconds() >= threshold_seconds


def promotion_plan(
    entries: Iterable[RecheckEntry],
    flights: dict[str, "Flight"],
    policy: RecheckPolicy,
    now: datetime,
) -> dict[str, str]:
    """Bag id -> promotion (``ageing``/``cutoff``) for entries due to move.

    A bag qualifies for at most one promotion: cutoff protection takes
    precedence, and an already promoted bag is skipped, which is what makes the
    one shot rule auditable directly from this plan.
    """

    plan: dict[str, str] = {}
    for entry in entries:
        deadline = cutoff_deadline(flights.get(entry.flight_id))
        if not entry.cutoff_guarded and near_cutoff(
            entry, deadline, now, policy.cutoff_lead_seconds
        ):
            plan[entry.bag_id] = "cutoff"
        elif over_age(entry, now, policy.ageing_after_seconds):
            plan[entry.bag_id] = "ageing"
    return plan
