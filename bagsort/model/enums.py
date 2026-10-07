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
    """Bands the recheck queue is served in, strictest first.

    NORMAL is the plain FIFO backlog. PRIORITY holds explicitly expedited bags
    and bags the aging policy moved forward once. PROTECTED is the cutoff floor:
    nothing (not even a head of queue expedite) is allowed to overtake it, so a
    bag close to its flight cutoff cannot be pushed back without limit.
    """

    NORMAL = "normal"
    PRIORITY = "priority"
    PROTECTED = "protected"

    @property
    def rank(self) -> int:
        """Smaller ranks are served first."""

        return _RECHECK_RANKS[self]


_RECHECK_RANKS = {
    RecheckPriority.PROTECTED: 0,
    RecheckPriority.PRIORITY: 10,
    RecheckPriority.NORMAL: 20,
}


class ExpeditePolicy(str, Enum):
    """Where an expedited bag enters relative to bags already waiting.

    HEAD claims the very front of the non-protected queue: it overtakes patient
    priority bags too, so they are the ones that go hungry and must be named.
    AHEAD_OF_NORMAL only overtakes plain queue bags; bags already waiting in the
    priority band keep their place ahead of the newcomer.
    """

    HEAD = "head"
    AHEAD_OF_NORMAL = "ahead_of_normal"


class PromotionCause(str, Enum):
    """Why a queued band moved to a stricter band."""

    AGING = "aging"
    CUTOFF = "cutoff"
    MANUAL = "manual"
