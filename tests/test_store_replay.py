"""Replay, snapshot integrity and restart."""

from __future__ import annotations

import json

import pytest

from bagsort.errors import ValidationError
from bagsort.store.snapshot import Envelope, build, digest_for
from bagsort.store.store import Store


def _seed(store: Store) -> None:
    with store.session() as session:
        session.record(
            "flight.put",
            {"flight": {"id": "T1-CA1", "code": "CA1", "state": "open"}},
        )
        session.record(
            "bag.put",
            {"bag": {"id": "T1-1", "barcode": "1", "sequence": 1, "flight_id": "T1-CA1"}},
        )
        session.record("mapping.put", {"bag_id": "T1-1", "flight_id": "T1-CA1"})


def test_replay_rebuilds_the_materialised_state(tmp_path):
    store = Store(str(tmp_path / "state.json"))
    _seed(store)
    live = store.state().as_dict()
    from bagsort.store.replay import replay

    assert replay(store.entries()).as_dict() == live


def test_persist_and_open_round_trip(tmp_path):
    path = tmp_path / "state.json"
    store = Store(str(path))
    _seed(store)
    digest = store.persist()
    assert store.dirty() is False
    raw = json.loads(path.read_text(encoding="utf-8"))
    assert raw["digest"] == digest
    assert raw["watermark"] == store.watermark

    reopened = Store.open(str(path))
    assert reopened.state().as_dict() == store.state().as_dict()
    assert reopened.watermark == store.watermark


def test_envelope_verify_rejects_a_tampered_digest(tmp_path):
    store = Store(str(tmp_path / "state.json"))
    _seed(store)
    envelope = store.envelope()
    assert envelope.verify() is True
    broken = Envelope(
        watermark=envelope.watermark,
        digest="deadbeef",
        entries=envelope.entries,
    )
    assert broken.verify() is False
    with pytest.raises(ValidationError):
        store.restore(broken)


def test_digest_changes_with_the_entry_stream(tmp_path):
    store = Store(str(tmp_path / "state.json"))
    empty = digest_for(store.entries())
    _seed(store)
    assert digest_for(store.entries()) != empty
    assert build(store.entries()).digest == digest_for(store.entries())


def test_reset_drops_the_journal(tmp_path):
    store = Store(str(tmp_path / "state.json"))
    _seed(store)
    store.reset()
    assert store.watermark == 0
    assert store.state().bags == {}
    assert store.dirty() is True


def test_open_missing_file_starts_empty(tmp_path):
    store = Store.open(str(tmp_path / "absent.json"))
    assert store.watermark == 0
    assert store.state().flights == {}


def test_invalid_snapshot_file_is_rejected(tmp_path):
    path = tmp_path / "state.json"
    path.write_text("{not json", encoding="utf-8")
    with pytest.raises(ValidationError):
        Store.open(str(path))


def test_legacy_recheck_enqueue_folds_as_a_normal_queue_entry(tmp_path):
    """Streams written before priority bands existed carry only the bag id."""

    store = Store(str(tmp_path / "state.json"))
    with store.session() as session:
        session.record(
            "bag.put",
            {"bag": {"id": "T1-1", "barcode": "1", "sequence": 1}},
        )
        session.record("recheck.enqueue", {"bag_id": "T1-1"})
    state = store.state()
    assert state.recheck_queue == ["T1-1"]
    entry = state.recheck_entries["T1-1"]
    assert entry.priority.value == "normal"
    assert entry.aging_promoted is False


def test_recheck_promote_and_reorder_replay_priority_order(tmp_path):
    from bagsort.model.enums import RecheckPriority

    store = Store(str(tmp_path / "state.json"))
    with store.session() as session:
        for bag_id, arrival in (("T1-a", 1), ("T1-b", 2)):
            session.record(
                "bag.put",
                {"bag": {"id": bag_id, "barcode": bag_id, "sequence": arrival}},
            )
            session.record(
                "recheck.enqueue",
                {
                    "entry": {
                        "bag_id": bag_id,
                        "arrival_seq": arrival,
                        "enqueued_at": "2024-01-01T00:00:00Z",
                        "effective_at": "2024-01-01T00:00:00Z",
                        "priority": "normal",
                    },
                    "position": arrival - 1,
                    "policy": "head",
                },
            )
        session.record(
            "recheck.promote",
            {
                "bag_id": "T1-b",
                "priority": "priority",
                "cause": "manual",
                "at": "2024-01-01T00:01:00Z",
            },
        )
        session.record("recheck.reorder", {"order": ["T1-b", "T1-a"]})
    state = store.state()
    assert state.recheck_queue == ["T1-b", "T1-a"]
    assert state.recheck_entries["T1-b"].priority is RecheckPriority.PRIORITY


def test_recheck_dequeue_removes_band_state_too(tmp_path):
    store = Store(str(tmp_path / "state.json"))
    with store.session() as session:
        session.record(
            "bag.put", {"bag": {"id": "T1-1", "barcode": "1", "sequence": 1}}
        )
        session.record(
            "recheck.enqueue",
            {"entry": {"bag_id": "T1-1", "arrival_seq": 1, "enqueued_at": "t"}},
        )
        session.record("recheck.dequeue", {"bag_id": "T1-1"})
    state = store.state()
    assert state.recheck_queue == []
    assert state.recheck_entries == {}

