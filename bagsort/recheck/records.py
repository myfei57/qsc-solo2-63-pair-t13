"""Structured results returned by priority recheck operations.

They exist so the audit trail, HTTP responses and tests can all read the same
fields instead of guessing at tuple positions. In particular every operation
that overtakes patient bags reports exactly who was starved.
"""

from __future__ import annotations

from dataclasses import dataclass, field

from bagsort.model.enums import ExpeditePolicy, PromotionCause, RecheckPriority


@dataclass(frozen=True)
class EnqueueResult:
    bag_id: str
    position: int
    length: int
    priority: RecheckPriority
    policy: ExpeditePolicy
    starved: list[str] = field(default_factory=list)

    def as_dict(self) -> dict[str, object]:
        return {
            "bag_id": self.bag_id,
            "position": self.position,
            "length": self.length,
            "priority": self.priority.value,
            "policy": self.policy.value,
            "starved": list(self.starved),
        }


@dataclass(frozen=True)
class Promotion:
    """One bag's automatic or operator driven band change."""

    bag_id: str
    priority: RecheckPriority
    cause: PromotionCause
    at: str
    waited_seconds: float = 0.0
    seconds_to_cutoff: float | None = None
    flight_id: str = ""
    cutoff_at: str = ""

    def as_dict(self) -> dict[str, object]:
        return {
            "bag_id": self.bag_id,
            "priority": self.priority.value,
            "cause": self.cause.value,
            "at": self.at,
            "waited_seconds": self.waited_seconds,
            "seconds_to_cutoff": self.seconds_to_cutoff,
            "flight_id": self.flight_id,
            "cutoff_at": self.cutoff_at,
        }


@dataclass(frozen=True)
class ReorderResult:
    """Outcome of one aging/cutoff reorder pass."""

    at: str
    order: list[str] = field(default_factory=list)
    promotions: list[Promotion] = field(default_factory=list)
    moved: list[str] = field(default_factory=list)

    def as_dict(self) -> dict[str, object]:
        return {
            "at": self.at,
            "order": list(self.order),
            "promotions": [promotion.as_dict() for promotion in self.promotions],
            "moved": list(self.moved),
        }


@dataclass(frozen=True)
class QueueRow:
    """One queue line joined with its flight cutoff for cross checking."""

    position: int
    bag_id: str
    flight_id: str
    priority: RecheckPriority
    enqueued_at: str
    waited_seconds: float
    cutoff_at: str
    seconds_to_cutoff: float | None
    aging_promoted: bool
    cutoff_promoted: bool

    def as_dict(self) -> dict[str, object]:
        return {
            "position": self.position,
            "bag_id": self.bag_id,
            "flight_id": self.flight_id,
            "priority": self.priority.value,
            "enqueued_at": self.enqueued_at,
            "waited_seconds": self.waited_seconds,
            "cutoff_at": self.cutoff_at,
            "seconds_to_cutoff": self.seconds_to_cutoff,
            "aging_promoted": self.aging_promoted,
            "cutoff_promoted": self.cutoff_promoted,
        }
