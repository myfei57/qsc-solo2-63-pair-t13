"""Enumerations for the two state machines the plant tracks."""

from __future__ import annotations

from enum import Enum


class FlightState(str, Enum):
    """Lifecycle of one flight batch."""

    OPEN = "open"
    BOARDING = "boarding"
    CUTOFF = "cutoff"
    CLOSED = "closed"


class BagState(str, Enum):
    """Lifecycle of one piece of baggage."""

    CHECKED_IN = "checked_in"
    INJECTED = "injected"
    SORTED = "sorted"
    RECHECKED = "rechecked"
    LOADED = "loaded"
    DIVERTED = "diverted"

    @property
    def active(self) -> bool:
        """Whether the bag still occupies plant capacity."""

        return self in (
            BagState.CHECKED_IN,
            BagState.INJECTED,
            BagState.SORTED,
            BagState.RECHECKED,
        )

class Verdict(str, Enum):
    """Result of comparing an observed (epoch, sequence) pair with the high-water mark."""

    FRESH = "fresh"
    REPLAYED = "replayed"
    ROLLED_OVER = "rolled_over"


class RecheckPriority(str, Enum):
    """Operator controlled urgency of a bag waiting for a second look."""

    NORMAL = "normal"
    URGENT = "urgent"


class UrgentPlacement(str, Enum):
    """Where an urgent arrival is allowed to cut in.

    ``BEFORE_NORMAL`` only lifts urgent bags ahead of ordinary FIFO waiters;
    bags already elevated (earlier urgents, age promoted bags) keep their place,
    so a steady stream of urgent traffic cannot starve them. ``HEAD`` puts
    urgent bags at the front of every non guarded class, which also pushes back
    age promoted waiters; the flight cutoff guard still outranks it.
    """

    HEAD = "head"
    BEFORE_NORMAL = "before_normal"
