"""The prioritised recheck backlog.

Covers FIFO basics, the two urgent insertion policies, ageing with its one shot
rule and arrival consistent ordering, the cutoff guard backstop, auditing of
every rearrangement and deterministic rebuild on restart.
"""

from __future__ import annotations

import pytest

from bagsort.app import Plant
from bagsort.errors import ConflictError, ValidationError
from bagsort.model.enums import BagState, RecheckPriority, UrgentPlacement
from bagsort.model.reorder import entry_class
from bagsort.store.store import Store
from tests.support import check_in


def _ids(plant):
    return [bag.id for bag in plant.recheck.items()]


def _enqueue(plant, bags, *indices, priority=RecheckPriority.NORMAL, placement=None):
    for index in indices:
        plant.enqueue_recheck(bags[index].id, priority, placement)


# -- FIFO basics -----------------------------------------------------------


def test_queue_processes_in_arrival_order(plant, flight):
    bags = check_in(plant, flight.id, 3)
    for bag in bags:
        plant.recheck.enqueue(bag)
    assert [bag.id for bag in plant.recheck.items()] == [bag.id for bag in bags]
    first = plant.recheck.dequeue()
    assert first.id == bags[0].id
    assert first.state is BagState.RECHECKED
    assert plant.recheck.length() == 2


def test_peek_does_not_remove(plant, flight):
    bags = check_in(plant, flight.id, 2)
    plant.recheck.enqueue(bags[0])
    assert plant.recheck.peek().id == bags[0].id
    assert plant.recheck.length() == 1


def test_empty_queue_returns_none(plant):
    assert plant.recheck.peek() is None
    assert plant.recheck.dequeue() is None
    assert plant.recheck.items() == []


def test_enqueue_is_idempotent(plant, flight):
    bag = check_in(plant, flight.id, 1)[0]
    plant.recheck.enqueue(bag)
    result = plant.recheck.enqueue(plant.bag(bag.id))
    assert plant.recheck.length() == 1
    assert result["idempotent"] is True


def test_clear_drops_the_backlog(plant, flight):
    bags = check_in(plant, flight.id, 2)
    for bag in bags:
        plant.recheck.enqueue(bag)
    assert plant.recheck.clear() == 2
    assert plant.recheck.length() == 0


def test_backlog_survives_a_restart(plant, flight):
    bags = check_in(plant, flight.id, 2)
    for bag in bags:
        plant.recheck.enqueue(bag)
    path = plant.store.persist()
    assert path
    reopened = Store.open(plant.store.path)
    assert reopened.state().recheck_queue == [bag.id for bag in bags]


def test_deadline_reads_the_flight_schedule(plant, flight):
    plant.book.set_cutoff(flight.id, "2024-01-01T12:00:00Z")
    assert plant.recheck.deadline(flight.id) == "2024-01-01T12:00:00Z"


# -- urgent insertion ------------------------------------------------------


def test_before_normal_urgent_overtakes_only_ordinary_waiters(plant, flight):
    bags = check_in(plant, flight.id, 4)
    _enqueue(plant, bags, 0, 1, 2)
    _, result = plant.enqueue_recheck(
        bags[3].id, RecheckPriority.URGENT, UrgentPlacement.BEFORE_NORMAL
    )
    assert _ids(plant) == [bags[3].id, bags[0].id, bags[1].id, bags[2].id]
    # the honest waiters are the ones pushed back, and the audit payload names them
    assert [item["bag_id"] for item in result["displaced"]] == [
        bags[0].id,
        bags[1].id,
        bags[2].id,
    ]


def test_head_urgent_also_starves_elevated_waiters_and_records_them(plant, flight):
    bags = check_in(plant, flight.id, 3)
    _enqueue(plant, bags, 0)
    _enqueue(plant, bags, 1, priority=RecheckPriority.URGENT,
             placement=UrgentPlacement.BEFORE_NORMAL)
    _, result = plant.enqueue_recheck(
        bags[2].id, RecheckPriority.URGENT, UrgentPlacement.HEAD
    )
    assert _ids(plant) == [bags[2].id, bags[1].id, bags[0].id]
    # with head placement the earlier urgent is itself pushed back -- that is the
    # starvation the operator explicitly opts into when choosing head
    assert [item["bag_id"] for item in result["displaced"]] == [bags[1].id, bags[0].id]


def test_before_normal_keeps_one_fifo_line_with_earlier_urgents(plant, flight):
    bags = check_in(plant, flight.id, 3)
    _enqueue(plant, bags, 0, priority=RecheckPriority.URGENT,
             placement=UrgentPlacement.BEFORE_NORMAL)
    _enqueue(plant, bags, 1, priority=RecheckPriority.URGENT,
             placement=UrgentPlacement.BEFORE_NORMAL)
    _enqueue(plant, bags, 2)
    assert _ids(plant) == [bags[0].id, bags[1].id, bags[2].id]


def test_default_placement_comes_from_the_queue_policy(plant, flight):
    plant.set_recheck_policy(urgent_placement=UrgentPlacement.HEAD)
    bags = check_in(plant, flight.id, 2)
    _enqueue(plant, bags, 0)
    bag, result = plant.enqueue_recheck(bags[1].id, RecheckPriority.URGENT)
    assert _ids(plant)[0] == bag.id
    assert result["class"] == "head_urgent"


# -- ageing ----------------------------------------------------------------


def test_ageing_lifts_an_old_waiter_once_and_keeps_class_fifo(plant, flight):
    bags = check_in(plant, flight.id, 4)
    _enqueue(plant, bags, 0, 1)
    plant.clock.advance(250)
    _enqueue(plant, bags, 2, priority=RecheckPriority.URGENT,
             placement=UrgentPlacement.BEFORE_NORMAL)
    plant.clock.advance(50)
    _enqueue(plant, bags, 3)
    # urgent leads the honest FIFO; nothing has aged yet
    assert _ids(plant) == [bags[2].id, bags[0].id, bags[1].id, bags[3].id]

    result = plant.rebalance_recheck()
    assert [move.reason for move in result["promotions"]] == ["ageing", "ageing"]
    # aged waiters (seq 1,2) lead the elevated FIFO line ahead of the later
    # urgent seq=3; the newest honest waiter (only 50s old) is not lifted
    assert _ids(plant) == [bags[0].id, bags[1].id, bags[2].id, bags[3].id]
    aged = plant.recheck.entry(bags[0].id)
    assert aged.age_promoted is True and aged.aged_at


def test_ageing_is_a_one_shot_promotion(plant, flight):
    bags = check_in(plant, flight.id, 2)
    _enqueue(plant, bags, 0, 1)
    plant.clock.advance(300)
    first = plant.rebalance_recheck()
    assert first["changed"] is True
    second = plant.rebalance_recheck()
    assert second["changed"] is False
    age_commands = [
        entry for entry in plant.store.entries() if entry.kind == "recheck.age"
    ]
    assert {entry.payload["bag_id"] for entry in age_commands} == {
        bags[0].id,
        bags[1].id,
    }


def test_ageing_threshold_is_configurable(plant, flight):
    plant.set_recheck_policy(ageing_after_seconds=60)
    bags = check_in(plant, flight.id, 1)
    _enqueue(plant, bags, 0)
    plant.clock.advance(59)
    assert plant.recheck.rebalance()["changed"] is False
    plant.clock.advance(1)
    result = plant.recheck.rebalance()
    assert result["changed"] is True
    assert result["promotions"][0].reason == "ageing"


def test_a_burst_of_urgents_cannot_starve_an_aged_waiter(plant, flight):
    bags = check_in(plant, flight.id, 4)
    _enqueue(plant, bags, 0)
    plant.clock.advance(300)
    plant.rebalance_recheck()
    for index in (1, 2, 3):
        plant.enqueue_recheck(
            bags[index].id, RecheckPriority.URGENT, UrgentPlacement.BEFORE_NORMAL
        )
    assert _ids(plant)[0] == bags[0].id


# -- cutoff guard -----------------------------------------------------------


def test_near_cutoff_bag_is_guarded_ahead_of_every_manual_class(plant):
    early = plant.register_flight("CA1", "CR1", "")
    urgent_flight = plant.register_flight("CA2", "CR2", "")
    # 630s out at t=0: outside the 600s guard window until time advances
    plant.book.set_cutoff(urgent_flight.id, "2024-01-01T00:10:30Z")

    honest = check_in(plant, early.id, 1)[0]
    urgent = check_in(plant, urgent_flight.id, 1, prefix="URG")[0]
    head = check_in(plant, early.id, 1, prefix="HEAD")[0]
    plant.enqueue_recheck(honest.id)
    plant.enqueue_recheck(urgent.id, RecheckPriority.URGENT, UrgentPlacement.BEFORE_NORMAL)
    plant.enqueue_recheck(head.id, RecheckPriority.URGENT, UrgentPlacement.HEAD)
    assert _ids(plant)[0] == head.id

    plant.clock.advance(40)  # deadline now 590s away, inside the guard window
    result = plant.rebalance_recheck()
    assert [(move.bag_id, move.reason) for move in result["promotions"]] == [
        (urgent.id, "cutoff")
    ]
    assert _ids(plant)[0] == urgent.id
    assert entry_class(plant.recheck.entry(urgent.id)) == "cutoff"
    # guarding twice does nothing
    assert plant.rebalance_recheck()["changed"] is False


def test_a_bag_enqueued_near_cutoff_is_guarded_immediately(plant):
    flight = plant.register_flight("CA9", "CR9", "")
    plant.book.set_cutoff(flight.id, "2024-01-01T00:00:30Z")
    bag = check_in(plant, flight.id, 1)[0]
    _, result = plant.enqueue_recheck(bag.id)
    assert result["class"] == "cutoff"
    assert result["promotions"][0].reason == "cutoff"


def test_cutoff_guard_outranks_head_urgents_arriving_later(plant):
    early = plant.register_flight("CA1", "CR1", "")
    due = plant.register_flight("CA2", "CR2", "")
    plant.book.set_cutoff(due.id, "2024-01-01T00:09:00Z")
    guarded = check_in(plant, due.id, 1)[0]
    plant.enqueue_recheck(guarded.id)
    plant.rebalance_recheck()
    for index in range(5):
        later = check_in(plant, early.id, 1, prefix=f"X{index}")[0]
        plant.enqueue_recheck(later.id, RecheckPriority.URGENT, UrgentPlacement.HEAD)
    assert _ids(plant)[0] == guarded.id


# -- priority changes ------------------------------------------------------


def test_priority_change_is_item_by_item_and_reversible(plant, flight):
    bags = check_in(plant, flight.id, 2)
    _enqueue(plant, bags, 0, 1)
    result = plant.set_recheck_priority(
        bags[1].id, RecheckPriority.URGENT, UrgentPlacement.BEFORE_NORMAL
    )
    assert result["changed"] is True
    assert _ids(plant) == [bags[1].id, bags[0].id]
    back = plant.set_recheck_priority(bags[1].id, RecheckPriority.NORMAL)
    assert back["changed"] is True
    assert _ids(plant) == [bags[0].id, bags[1].id]


def test_priority_change_is_a_noop_when_unchanged(plant, flight):
    bags = check_in(plant, flight.id, 1)
    _enqueue(plant, bags, 0)
    result = plant.set_recheck_priority(bags[0].id, RecheckPriority.NORMAL)
    assert result["changed"] is False


def test_priority_change_requires_a_queued_bag(plant, flight):
    bag = check_in(plant, flight.id, 1)[0]
    with pytest.raises(ConflictError):
        plant.set_recheck_priority(bag.id, RecheckPriority.URGENT)


def test_age_promoted_bag_cannot_be_demoted_out_of_its_promotion(plant, flight):
    bags = check_in(plant, flight.id, 2)
    _enqueue(plant, bags, 0, 1)
    plant.clock.advance(300)
    plant.rebalance_recheck()
    plant.set_recheck_priority(bags[1].id, RecheckPriority.URGENT, UrgentPlacement.HEAD)
    # aged bag keeps the elevated line behind the head urgent; a later head
    # urgent can lead, but demoting the aged bag's operator priority does not
    # erase the automation mark
    plant.set_recheck_priority(bags[0].id, RecheckPriority.NORMAL)
    assert plant.recheck.entry(bags[0].id).age_promoted is True


# -- policy ----------------------------------------------------------------


def test_policy_is_persisted_and_validated(plant, flight):
    policy = plant.set_recheck_policy(ageing_after_seconds=120, cutoff_lead_seconds=900)
    assert policy.ageing_after_seconds == 120
    assert plant.recheck.policy().cutoff_lead_seconds == 900
    with pytest.raises(ValidationError):
        plant.set_recheck_policy(ageing_after_seconds=-1)


# -- audit trail ------------------------------------------------------------


def test_every_rearrangement_leaves_a_linked_audit_entry(plant, flight):
    bags = check_in(plant, flight.id, 4)
    _enqueue(plant, bags, 0)
    plant.clock.advance(299)
    _enqueue(plant, bags, 1)
    plant.clock.advance(1)
    # urgent leads for now; an old honest waiter and a fresh bystander wait below
    _enqueue(plant, bags, 2, priority=RecheckPriority.URGENT,
             placement=UrgentPlacement.BEFORE_NORMAL)
    _enqueue(plant, bags, 3)
    plant.rebalance_recheck()  # only bags[0] is 300s old; it overtakes the urgent
    plant.set_recheck_priority(
        bags[1].id, RecheckPriority.URGENT, UrgentPlacement.BEFORE_NORMAL
    )
    kinds = [entry.kind for entry in plant.audit.for_subject(bags[0].id)]
    assert kinds == ["checkin", "recheck", "recheck_ageing"]
    other = [entry.kind for entry in plant.audit.for_subject(bags[1].id)]
    assert other == ["checkin", "recheck", "recheck_priority"]
    reorders = plant.audit.for_subject("queue")
    triggers = [entry.detail["trigger"] for entry in reorders]
    # urgent enqueue, then the ageing rebalance that overtakes it, then the
    # item by item priority change -- each leaves a queue level audit record
    assert triggers == ["enqueue", "ageing", "priority"]
    ageing_entry = next(entry for entry in reorders if entry.detail["trigger"] == "ageing")
    displaced = {item["bag_id"] for item in ageing_entry.detail["displaced"]}
    assert bags[2].id in displaced
    assert plant.audit.verify() is True


def test_cutoff_promotion_audit_carries_the_deadline(plant):
    flight = plant.register_flight("CA2", "CR2", "")
    plant.book.set_cutoff(flight.id, "2024-01-01T00:05:00Z")
    bag = check_in(plant, flight.id, 1)[0]
    plant.enqueue_recheck(bag.id)
    entry = next(
        record for record in plant.audit.for_subject(bag.id)
        if record.kind == "recheck_cutoff"
    )
    assert entry.detail["cutoff_at"] == "2024-01-01T00:05:00Z"
    assert entry.detail["to_class"] == "cutoff"


def test_processing_a_bag_is_audited(plant, flight):
    bag = check_in(plant, flight.id, 1)[0]
    plant.enqueue_recheck(bag.id)
    plant.process_recheck()
    kinds = [entry.kind for entry in plant.audit.for_subject(bag.id)]
    assert "recheck_processed" in kinds


# -- cross check and restart -----------------------------------------------


def test_crosscheck_joins_order_cutoff_and_audit_cleanly(plant):
    flight = plant.register_flight("CA2", "CR2", "")
    plant.book.set_cutoff(flight.id, "2024-01-01T00:05:00Z")
    bag = check_in(plant, flight.id, 1)[0]
    plant.enqueue_recheck(bag.id)
    report = plant.recheck_crosscheck()
    assert report["ok"] is True
    assert report["issues"] == []
    assert report["audit_intact"] is True
    assert report["order"][0]["deadline"] == "2024-01-01T00:05:00Z"
    assert report["order"][0]["class"] == "cutoff"


def test_crosscheck_flags_an_unprotected_near_cutoff_bag(plant):
    flight = plant.register_flight("CA2", "CR2", "")
    plant.book.set_cutoff(flight.id, "2024-01-01T00:05:00Z")
    bag = check_in(plant, flight.id, 1)[0]
    # enqueue with a wide guard window so nothing is protected yet
    plant.set_recheck_policy(cutoff_lead_seconds=0)
    plant.enqueue_recheck(bag.id)
    assert plant.recheck_crosscheck()["ok"] is True
    # tighten the window without rebalancing: the bag is now due a guard
    plant.set_recheck_policy(cutoff_lead_seconds=600)
    report = plant.recheck_crosscheck()
    codes = {issue["code"] for issue in report["issues"]}
    assert "cutoff_unprotected" in codes


def test_prioritised_order_is_deterministic_after_restart(plant):
    early = plant.register_flight("CA1", "CR1", "")
    due = plant.register_flight("CA2", "CR2", "")
    plant.book.set_cutoff(due.id, "2024-01-01T00:09:30Z")
    normals = check_in(plant, early.id, 2)
    urgent = check_in(plant, early.id, 1, prefix="U")[0]
    guarded = check_in(plant, due.id, 1, prefix="C")[0]
    for bag in normals:
        plant.enqueue_recheck(bag.id)
    plant.enqueue_recheck(urgent.id, RecheckPriority.URGENT, UrgentPlacement.HEAD)
    plant.clock.advance(300)
    plant.rebalance_recheck()
    plant.enqueue_recheck(guarded.id)

    expected = _ids(plant)
    plant.store.persist()
    reopened = Store.open(plant.store.path)
    assert reopened.state().recheck_queue == expected
    rebuilt = Plant.build(reopened, clock=plant.clock)
    assert [row["bag_id"] for row in rebuilt.recheck.order()] == expected
    assert rebuilt.recheck_crosscheck()["ok"] is True


def test_legacy_fifo_journal_rebuilds_in_enqueue_order(tmp_path):
    """Snapshots written before priorities existed keep their FIFO order."""

    store = Store(str(tmp_path / "state.json"))
    with store.session() as session:
        for index in range(3):
            session.record(
                "bag.put",
                {"bag": {"id": f"T1-{index}", "barcode": str(index), "sequence": index}},
            )
            session.record("recheck.enqueue", {"bag_id": f"T1-{index}"})
    assert store.state().recheck_queue == ["T1-0", "T1-1", "T1-2"]


def test_dequeue_runs_promotions_so_the_guard_cannot_be_left_behind(plant):
    early = plant.register_flight("CA1", "CR1", "")
    due = plant.register_flight("CA2", "CR2", "")
    # 630s out at t=0: the head urgent leads until the window is entered
    plant.book.set_cutoff(due.id, "2024-01-01T00:10:30Z")
    head = check_in(plant, early.id, 1)[0]
    guarded = check_in(plant, due.id, 1, prefix="C")[0]
    plant.enqueue_recheck(head.id, RecheckPriority.URGENT, UrgentPlacement.HEAD)
    plant.enqueue_recheck(guarded.id)
    assert plant.recheck.peek().id == head.id
    plant.clock.advance(40)  # deadline now 590s out, guard becomes due
    processed = plant.process_recheck()
    assert processed.id == head.id
    # the very next serve is the guarded bag, even though nobody rebalanced by hand
    assert plant.recheck.peek().id == guarded.id
    assert plant.process_recheck().id == guarded.id
