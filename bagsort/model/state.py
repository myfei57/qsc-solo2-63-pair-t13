"""The folded document every subsystem reads and the reducer that builds it.

The store keeps an append-only journal of committed commands. This module turns
that journal into the materialised state and knows how to apply one command,
which is what makes restart-by-replay possible.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from .entities import (
    AuditEntry,
    Bag,
    ChuteAssignment,
    Flight,
    LoadRecord,
    RecheckEntry,
    Sequence,
    SortRecord,
    TransferCommand,
)
from bagsort.errors import ValidationError
from .enums import RecheckPriority


@dataclass
class State:
    """Materialised view of every committed command."""

    flights: dict[str, Flight] = field(default_factory=dict)
    bags: dict[str, Bag] = field(default_factory=dict)
    mappings: dict[str, str] = field(default_factory=dict)
    assignments: dict[str, ChuteAssignment] = field(default_factory=dict)
    sort_records: list[SortRecord] = field(default_factory=list)
    transfer_commands: list[TransferCommand] = field(default_factory=list)
    load_records: list[LoadRecord] = field(default_factory=list)
    audit: list[AuditEntry] = field(default_factory=list)
    quota_limits: dict[str, int] = field(default_factory=dict)
    counters: dict[str, int] = field(default_factory=dict)
    sequence: Sequence = field(default_factory=Sequence)
    recheck_queue: list[str] = field(default_factory=list)
    recheck_entries: dict[str, RecheckEntry] = field(default_factory=dict)
    recheck_arrival_seq: int = 0
    belt_carousel: str = ""
    belt_batch: int = 0
    belt_on_carousel: list[str] = field(default_factory=list)

    def copy(self) -> "State":
        return State(
            flights=dict(self.flights),
            bags=dict(self.bags),
            mappings=dict(self.mappings),
            assignments=dict(self.assignments),
            sort_records=list(self.sort_records),
            transfer_commands=list(self.transfer_commands),
            load_records=list(self.load_records),
            audit=list(self.audit),
            quota_limits=dict(self.quota_limits),
            counters=dict(self.counters),
            sequence=self.sequence,
            recheck_queue=list(self.recheck_queue),
            recheck_entries=dict(self.recheck_entries),
            recheck_arrival_seq=self.recheck_arrival_seq,
            belt_carousel=self.belt_carousel,
            belt_batch=self.belt_batch,
            belt_on_carousel=list(self.belt_on_carousel),
        )

    def as_dict(self) -> dict[str, Any]:
        return {
            "flights": {key: value.as_dict() for key, value in sorted(self.flights.items())},
            "bags": {key: value.as_dict() for key, value in sorted(self.bags.items())},
            "mappings": dict(sorted(self.mappings.items())),
            "assignments": {
                key: value.as_dict() for key, value in sorted(self.assignments.items())
            },
            "sort_records": [record.as_dict() for record in self.sort_records],
            "transfer_commands": [command.as_dict() for command in self.transfer_commands],
            "load_records": [record.as_dict() for record in self.load_records],
            "audit": [entry.as_dict() for entry in self.audit],
            "quota_limits": dict(sorted(self.quota_limits.items())),
            "counters": dict(sorted(self.counters.items())),
            "sequence": self.sequence.as_dict(),
            "recheck_queue": list(self.recheck_queue),
            "recheck_entries": {
                key: entry.as_dict() for key, entry in sorted(self.recheck_entries.items())
            },
            "recheck_arrival_seq": self.recheck_arrival_seq,
            "belt": {
                "carousel": self.belt_carousel,
                "batch": self.belt_batch,
                "on_carousel": list(self.belt_on_carousel),
            },
        }

def apply_entry(state: State, kind: str, payload: dict[str, Any]) -> None:
    """Apply one journal command to ``state`` in place."""

    if kind == "flight.put":
        flight = Flight.from_dict(payload["flight"])
        state.flights[flight.id] = flight
    elif kind == "flight.remove":
        state.flights.pop(str(payload["flight_id"]), None)
        state.mappings = {
            bag_id: flight_id
            for bag_id, flight_id in state.mappings.items()
            if flight_id != str(payload["flight_id"])
        }
    elif kind == "chute.assign":
        assignment = ChuteAssignment.from_dict(payload)
        state.assignments[assignment.flight_id] = assignment
    elif kind == "bag.put":
        bag = Bag.from_dict(payload["bag"])
        state.bags[bag.id] = bag
    elif kind == "bag.remove":
        state.bags.pop(str(payload["bag_id"]), None)
        state.mappings.pop(str(payload["bag_id"]), None)
    elif kind == "mapping.put":
        state.mappings[str(payload["bag_id"])] = str(payload["flight_id"])
    elif kind == "sequence.put":
        state.sequence = Sequence.from_dict(payload)
    elif kind == "sort.record":
        state.sort_records.append(SortRecord.from_dict(payload["record"]))
    elif kind == "transfer.append":
        state.transfer_commands.append(TransferCommand.from_dict(payload["command"]))
    elif kind == "transfer.clear_before":
        floor = int(payload["batch"])
        state.transfer_commands = [
            command for command in state.transfer_commands if command.batch >= floor
        ]
    elif kind == "load.append":
        state.load_records.append(LoadRecord.from_dict(payload["record"]))
    elif kind == "audit.append":
        state.audit.append(AuditEntry.from_dict(payload["entry"]))
    elif kind == "quota.set":
        state.quota_limits[str(payload["flight_id"])] = int(payload["limit"])
    elif kind == "quota.adjust":
        flight_id = str(payload["flight_id"])
        base = state.quota_limits.get(flight_id, 0)
        state.quota_limits[flight_id] = base + int(payload["delta"])
    elif kind == "counter.add":
        name = str(payload["name"])
        state.counters[name] = state.counters.get(name, 0) + int(payload["delta"])
    elif kind == "recheck.enqueue":
        if "entry" in payload:
            entry = RecheckEntry.from_dict(payload["entry"])
        else:
            # Streams folded before priorities existed only carried the bag id.
            entry = RecheckEntry(
                bag_id=str(payload["bag_id"]),
                arrival_seq=int(payload.get("arrival_seq", len(state.recheck_queue))),
                enqueued_at=str(payload.get("enqueued_at", "")),
            )
        state.recheck_entries[entry.bag_id] = entry
        state.recheck_arrival_seq = max(state.recheck_arrival_seq, entry.arrival_seq)
        if entry.bag_id not in state.recheck_queue:
            position = payload.get("position")
            if isinstance(position, int):
                state.recheck_queue.insert(min(position, len(state.recheck_queue)), entry.bag_id)
            else:
                state.recheck_queue.append(entry.bag_id)
    elif kind == "recheck.promote":
        bag_id = str(payload["bag_id"])
        entry = state.recheck_entries.get(bag_id)
        if entry is not None:
            promoted_at = str(payload.get("at", ""))
            state.recheck_entries[bag_id] = entry.evolved(
                priority=RecheckPriority(str(payload["priority"])),
                effective_at=promoted_at or entry.effective_at,
                aging_promoted=entry.aging_promoted or bool(payload.get("aging_promoted", False)),
                cutoff_promoted=entry.cutoff_promoted
                or bool(payload.get("cutoff_promoted", False)),
                last_promoted_at=promoted_at or entry.last_promoted_at,
            )
    elif kind == "recheck.reorder":
        order = [str(item) for item in payload.get("order", ())]
        queued = [item for item in order if item in state.recheck_entries]
        queued.extend(item for item in state.recheck_queue if item not in queued)
        state.recheck_queue = queued
    elif kind == "recheck.dequeue":
        bag_id = str(payload["bag_id"])
        state.recheck_queue = [item for item in state.recheck_queue if item != bag_id]
        state.recheck_entries.pop(bag_id, None)
    elif kind == "recheck.clear":
        state.recheck_queue = []
        state.recheck_entries = {}
    elif kind == "belt.set":
        state.belt_carousel = str(payload.get("carousel", ""))
        state.belt_batch = int(payload.get("batch", 0))
    elif kind == "belt.place":
        bag_id = str(payload["bag_id"])
        if bag_id not in state.belt_on_carousel:
            state.belt_on_carousel.append(bag_id)
    elif kind == "belt.clear":
        state.belt_on_carousel = []
        state.belt_carousel = str(payload.get("carousel", state.belt_carousel))
        state.belt_batch = int(payload.get("batch", state.belt_batch))
    else:  # pragma: no cover - guards against a typo in a new command kind
        raise ValidationError(f"unknown journal command {kind!r}")


def fold(entries: list[tuple[str, dict[str, Any]]]) -> State:
    """Rebuild a :class:`State` from a sequence of ``(kind, payload)`` pairs."""

    state = State()
    for kind, payload in entries:
        apply_entry(state, kind, payload)
    return state
