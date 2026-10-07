"""Tuning for the priority recheck queue.

Two independent levers decide when a patiently waiting bag moves forward:

* ``aging_after_seconds``: pure wait time. A plain queue bag older than this is
  lifted into the priority band exactly once, so a stream of expedited bags
  cannot starve it forever.
* ``cutoff_within_seconds``: time to flight cutoff. A bag whose cutoff is at
  least this close is lifted to the protected band, the floor no expedite may
  overtake, so rate limiting cannot push it past its flight's deadline.
"""

from __future__ import annotations

from dataclasses import dataclass

DEFAULT_AGING_AFTER_SECONDS = 120
DEFAULT_CUTOFF_WITHIN_SECONDS = 60


@dataclass(frozen=True)
class AgingPolicy:
    """Thresholds the reorder scan applies."""

    aging_after_seconds: float = DEFAULT_AGING_AFTER_SECONDS
    cutoff_within_seconds: float = DEFAULT_CUTOFF_WITHIN_SECONDS

    def __post_init__(self) -> None:
        if self.aging_after_seconds < 0:
            raise ValueError("aging_after_seconds must not be negative")
        if self.cutoff_within_seconds < 0:
            raise ValueError("cutoff_within_seconds must not be negative")

    def aged_out(self, waited_seconds: float) -> bool:
        return waited_seconds >= self.aging_after_seconds

    def cutoff_near(self, seconds_to_cutoff: float) -> bool:
        return 0.0 <= seconds_to_cutoff <= self.cutoff_within_seconds
