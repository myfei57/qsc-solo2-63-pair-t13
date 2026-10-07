"""The priority recheck backlog, aging pass and cutoff protection."""

from __future__ import annotations

import pytest

from bagsort.errors import ConflictError, NotFoundError
from bagsort.app import Plant
from bagsort.model.enums import BagState, ExpeditePolicy, PromotionCause, RecheckPriority
from bagsort.recheck.policy import AgingPolicy
from bagsort.store.store import Store
from tests.support import check_in


def _ids(results):
    return [bag.id for bag in results]


# --------------------------------------------------------------------------
# FIFO baseline
# --------------------------------------------------------------------------


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
    replay = plant.recheck.enqueue(plant.bag(bag.id))
    assert plant.recheck.length() == 1
    assert replay.starved == []
    assert replay.position == 0


def test_clear_drops_the_backlog(plant, flight):
    bags = check_in(plant, flight.id, 2)
    for bag in bags:
        plant.recheck.enqueue(bag)
    assert plant.recheck.clear() == 2
    assert plant.recheck.length() == 0
    assert plant.recheck.entries() == []


def test_backlog_survives_a_restart(plant, flight):
    bags = check_in(plant, flight.id, 2)
    for bag in bags:
        plant.recheck.enqueue(bag)
    plant.store.persist()
    reopened = Store.open(plant.store.path)
    assert reopened.state().recheck_queue == [bag.id for bag in bags]
    assert [
        reopened.state().recheck_entries[bag.id].priority for bag in bags
    ] == [RecheckPriority.NORMAL, RecheckPriority.NORMAL]


def test_deadline_reads_the_flight_schedule(plant, flight):
    plant.book.set_cutoff(flight.id, "2024-01-01T12:00:00Z")
    assert plant.recheck.deadline(flight.id) == "2024-01-01T12:00:00Z"


# --------------------------------------------------------------------------
# Expedite: head vs. ahead-of-normal
# --------------------------------------------------------------------------


def test_head_expedite_overtakes_every_non_protected_waiter(plant, flight):
    bags = check_in(plant, flight.id, 3)
    plant.enqueue_recheck(bags[0].id)
    plant.enqueue_recheck(bags[1].id)
    result = plant.enqueue_recheck(
        bags[2].id, RecheckPriority.PRIORITY, ExpeditePolicy.HEAD
    )
    assert result.position == 0
    assert result.starved == [bags[0].id, bags[1].id]
    assert _ids(plant.recheck.items()) == [bags[2].id, bags[0].id, bags[1].id]
    assert plant.recheck.peek().id == bags[2].id


def test_ahead_of_normal_only_overtakes_plain_waiters(plant, flight):
    bags = check_in(plant, flight.id, 4)
    # Existing priority waiter must keep its place ahead of the newcomer.
    plant.enqueue_recheck(bags[0].id, RecheckPriority.PRIORITY, ExpeditePolicy.HEAD)
    plant.enqueue_recheck(bags[1].id)
    plant.enqueue_recheck(bags[2].id)
    result = plant.enqueue_recheck(
        bags[3].id, RecheckPriority.PRIORITY, ExpeditePolicy.AHEAD_OF_NORMAL
    )
    assert result.position == 1
    assert result.starved == [bags[1].id, bags[2].id]
    assert _ids(plant.recheck.items()) == [
        bags[0].id,
        bags[3].id,
        bags[1].id,
        bags[2].id,
    ]


def test_no_expedite_crosses_the_protected_floor(plant, flight):
    bags = check_in(plant, flight.id, 3)
    plant.enqueue_recheck(bags[0].id, RecheckPriority.PROTECTED)
    plant.enqueue_recheck(bags[1].id)
    result = plant.enqueue_recheck(
        bags[2].id, RecheckPriority.PRIORITY, ExpeditePolicy.HEAD
    )
    assert result.position == 1
    assert result.starved == [bags[1].id]
    assert _ids(plant.recheck.items())[0] == bags[0].id


def test_expedite_existing_bag_names_who_goes_hungry(plant, flight):
    bags = check_in(plant, flight.id, 3)
    for bag in bags:
        plant.enqueue_recheck(bag.id)
    result = plant.expedite_recheck(bags[2].id, ExpeditePolicy.HEAD)
    assert result.starved == [bags[0].id, bags[1].id]
    assert plant.recheck.entry(bags[2].id).priority is RecheckPriority.PRIORITY


def test_expedite_ahead_of_normal_leaves_promoted_waiters_untouched(plant, flight):
    bags = check_in(plant, flight.id, 3)
    plant.enqueue_recheck(bags[0].id)
    plant.enqueue_recheck(bags[1].id)
    # bags[1] already aged into the priority band once.
    plant.promote_recheck(bags[1].id, RecheckPriority.PRIORITY, PromotionCause.AGING)
    plant.enqueue_recheck(bags[2].id)
    result = plant.expedite_recheck(bags[2].id, ExpeditePolicy.AHEAD_OF_NORMAL)
    assert result.starved == [bags[0].id]
    assert _ids(plant.recheck.items()) == [bags[1].id, bags[2].id, bags[0].id]


def test_expedite_a_protected_bag_is_rejected(plant, flight):
    bag = check_in(plant, flight.id, 1)[0]
    plant.enqueue_recheck(bag.id, RecheckPriority.PROTECTED)
    with pytest.raises(ConflictError):
        plant.expedite_recheck(bag.id, ExpeditePolicy.HEAD)


def test_expedite_unknown_bag_is_not_found(plant):
    with pytest.raises(NotFoundError):
        plant.recheck.expedite("T1-nope")


# --------------------------------------------------------------------------
# Aging pass
# --------------------------------------------------------------------------


def test_aging_lifts_a_plain_waiter_without_reordering_arrival_order(plant, flight):
    bags = check_in(plant, flight.id, 3)
    for bag in bags:
        plant.enqueue_recheck(bag.id)
    plant.clock.advance(119)
    assert plant.reorder_recheck().promotions == []
    plant.clock.advance(1)
    result = plant.reorder_recheck()
    assert [promotion.bag_id for promotion in result.promotions] == [bag.id for bag in bags]
    assert {promotion.cause for promotion in result.promotions} == {PromotionCause.AGING}
    assert _ids(plant.recheck.items()) == [bag.id for bag in bags]
    for entry in plant.recheck.entries():
        assert entry.priority is RecheckPriority.PRIORITY
        assert entry.aging_promoted is True


def test_a_bag_is_aged_promoted_at_most_once(plant, flight):
    bag = check_in(plant, flight.id, 1)[0]
    plant.enqueue_recheck(bag.id)
    plant.clock.advance(120)
    first = plant.reorder_recheck()
    assert len(first.promotions) == 1
    plant.clock.advance(600)
    second = plant.reorder_recheck()
    assert second.promotions == []
    assert plant.recheck.entry(bag.id).aging_promoted is True


def test_aging_keeps_same_band_arrival_consistency(plant, flight):
    bags = check_in(plant, flight.id, 4)
    plant.enqueue_recheck(bags[0].id, RecheckPriority.PRIORITY, ExpeditePolicy.HEAD)
    plant.enqueue_recheck(bags[1].id)
    plant.clock.advance(120)
    plant.enqueue_recheck(bags[2].id, RecheckPriority.PRIORITY, ExpeditePolicy.HEAD)
    plant.enqueue_recheck(bags[3].id)
    plant.reorder_recheck()
    ids = [entry.bag_id for entry in plant.recheck.entries()]
    # Priority band: original expedite, the aged normal (arrival 2), the new
    # expedite (arrival 3) — all by arrival, not by who jumped last.
    assert ids == [bag.id for bag in bags]
    bands = [entry.priority for entry in plant.recheck.entries()]
    assert bands == [
        RecheckPriority.PRIORITY,
        RecheckPriority.PRIORITY,
        RecheckPriority.PRIORITY,
        RecheckPriority.NORMAL,
    ]


def test_aging_prevents_starvation_by_a_stream_of_expedites(plant, flight):
    patient = check_in(plant, flight.id, 1, prefix="OLD")[0]
    plant.enqueue_recheck(patient.id)
    plant.clock.advance(120)
    rush = check_in(plant, flight.id, 3, prefix="NEW")
    for bag in rush:
        plant.enqueue_recheck(bag.id, RecheckPriority.PRIORITY, ExpeditePolicy.HEAD)
    # Every head expedite claims the front, so before the reorder the last
    # arrival is on top and the patient bag is starved.
    assert plant.recheck.peek().id == rush[-1].id
    plant.reorder_recheck()
    # The patient bag has aged into the priority band and is served first,
    # ahead of every expedite that only arrived after it, and the expedites
    # themselves keep arrival order among one another.
    served = [bag.id for bag in plant.recheck.items()]
    assert served == [patient.id] + [bag.id for bag in rush]


# --------------------------------------------------------------------------
# Cutoff protection
# --------------------------------------------------------------------------


def test_protected_band_keeps_arrival_order_and_blocks_head_jumps(plant, flight):
    near_bags = check_in(plant, flight.id, 2, prefix="NEAR")
    other = check_in(plant, flight.id, 1, prefix="PLAIN")[0]
    plant.enqueue_recheck(near_bags[0].id, RecheckPriority.PROTECTED)
    plant.enqueue_recheck(near_bags[1].id, RecheckPriority.PROTECTED)
    plant.enqueue_recheck(other.id)
    # Protected arrivals stay in arrival order ahead of everyone else.
    assert _ids(plant.recheck.items()) == [
        near_bags[0].id,
        near_bags[1].id,
        other.id,
    ]
    pushed = plant.recheck.enqueue(plant.bag(other.id))
    assert pushed.length == 3  # idempotent replay adds nothing
    rush = check_in(plant, flight.id, 1, prefix="RUSH")[0]
    head = plant.enqueue_recheck(rush.id, RecheckPriority.PRIORITY, ExpeditePolicy.HEAD)
    assert head.position == 2
    assert _ids(plant.recheck.items())[:2] == [near_bags[0].id, near_bags[1].id]


def test_near_cutoff_bag_is_protected_against_later_expedites(plant, flight):
    plant.book.set_cutoff(flight.id, "2024-01-01T00:01:00Z")
    urgent = check_in(plant, flight.id, 1, prefix="NEAR")[0]
    plant.enqueue_recheck(urgent.id)
    plant.clock.advance(45)
    result = plant.reorder_recheck()
    assert len(result.promotions) == 1
    assert result.promotions[0].cause is PromotionCause.CUTOFF
    assert result.promotions[0].priority is RecheckPriority.PROTECTED

    later = check_in(plant, flight.id, 1, prefix="RUSH")[0]
    pushed = plant.enqueue_recheck(
        later.id, RecheckPriority.PRIORITY, ExpeditePolicy.HEAD
    )
    assert pushed.position == 1
    assert plant.recheck.peek().id == urgent.id


def test_rate_limiting_cannot_push_a_near_cutoff_bag_back_without_limit(plant, flight):
    plant.book.set_cutoff(flight.id, "2024-01-01T00:01:00Z")
    near = check_in(plant, flight.id, 1, prefix="NEAR")[0]
    plant.enqueue_recheck(near.id)
    plant.clock.advance(45)
    plant.reorder_recheck()
    # Any number of later head expedites keep landing behind the protected bag.
    for index in range(5):
        newcomer = check_in(plant, flight.id, 1, prefix=f"R{index}")[0]
        plant.enqueue_recheck(newcomer.id, RecheckPriority.PRIORITY, ExpeditePolicy.HEAD)
    assert plant.recheck.peek().id == near.id
    assert plant.recheck.length() == 6


def test_cutoff_promotion_happens_once(plant, flight):
    plant.book.set_cutoff(flight.id, "2024-01-01T00:02:00Z")
    bag = check_in(plant, flight.id, 1)[0]
    plant.enqueue_recheck(bag.id)
    plant.clock.advance(90)  # 30s to cutoff, also past the aging threshold
    first = plant.reorder_recheck()
    assert len(first.promotions) == 1
    assert first.promotions[0].cause is PromotionCause.CUTOFF
    plant.clock.advance(20)
    assert plant.reorder_recheck().promotions == []
    entry = plant.recheck.entry(bag.id)
    assert entry.cutoff_promoted is True


def test_passed_cutoff_is_not_reopened_and_no_false_promotion(plant, flight):
    plant.book.set_cutoff(flight.id, "2024-01-01T00:00:30Z")
    bag = check_in(plant, flight.id, 1)[0]
    plant.enqueue_recheck(bag.id)
    plant.clock.advance(60)
    result = plant.reorder_recheck()
    # Cutoff already passed (negative remaining) → no cutoff promotion; the
    # bag still ages normally on its own time signal.
    assert all(promotion.cause is PromotionCause.AGING for promotion in result.promotions)


def test_bag_without_cutoff_only_ages_on_time(plant, flight):
    assert plant.book.get(flight.id).cutoff_at == ""
    bag = check_in(plant, flight.id, 1)[0]
    plant.enqueue_recheck(bag.id)
    plant.clock.advance(120)
    result = plant.reorder_recheck()
    assert len(result.promotions) == 1
    assert result.promotions[0].cause is PromotionCause.AGING


# --------------------------------------------------------------------------
# Manual changes and review trail
# --------------------------------------------------------------------------


def test_aging_reorder_pushes_later_head_jumps_back_behind_patient_bags(plant, flight):
    patient = check_in(plant, flight.id, 1, prefix="PAT")[0]
    plant.enqueue_recheck(patient.id)
    plant.clock.advance(60)
    urgent = check_in(plant, flight.id, 1, prefix="URG")[0]
    plant.enqueue_recheck(urgent.id, RecheckPriority.PRIORITY, ExpeditePolicy.HEAD)
    # Before aging the head jump is in front.
    assert plant.recheck.peek().id == urgent.id
    # The patient bag crosses the aging threshold; the reorder re-asserts
    # arrival order and puts it ahead of the later head jump.
    plant.clock.advance(60)
    result = plant.reorder_recheck()
    assert [promotion.bag_id for promotion in result.promotions] == [patient.id]
    assert patient.id in result.moved
    assert plant.recheck.peek().id == patient.id
    promoted = plant.recheck.entry(urgent.id)
    assert promoted.priority is RecheckPriority.PRIORITY


def test_noop_reorder_keeps_the_head_expedite_in_place_but_is_audited(plant, flight):
    bags = check_in(plant, flight.id, 2)
    plant.enqueue_recheck(bags[0].id)
    plant.enqueue_recheck(bags[1].id)
    plant.expedite_recheck(bags[1].id, ExpeditePolicy.HEAD)
    before = [bag.id for bag in plant.recheck.items()]
    assert before == [bags[1].id, bags[0].id]
    # Nothing has aged yet: the pass moves no bag and must not silently undo
    # the operator's head jump.
    result = plant.reorder_recheck()
    assert result.promotions == []
    assert result.moved == []
    assert [bag.id for bag in plant.recheck.items()] == before
    # Every reorder still leaves an audit entry, even an unchanged one.
    reorder_entries = [
        entry for entry in plant.audit.entries() if entry.kind == "recheck_reorder"
    ]
    assert len(reorder_entries) == 1
    assert reorder_entries[0].detail["promotions"] == []


def test_manual_promotion_is_reviewable_one_entry_at_a_time(plant, flight):
    bags = check_in(plant, flight.id, 2)
    for bag in bags:
        plant.enqueue_recheck(bag.id)
    plant.promote_recheck(bags[1].id, RecheckPriority.PRIORITY)
    plant.promote_recheck(bags[0].id, RecheckPriority.PROTECTED)
    history = plant.recheck.promotion_history()
    assert [row["bag_id"] for row in history] == [bags[1].id, bags[0].id]
    assert [row["cause"] for row in history] == ["manual", "manual"]
    assert [row["priority"] for row in history] == ["priority", "protected"]
    assert all("sequence" in row and "at" in row for row in history)
    assert _ids(plant.recheck.items()) == [bags[0].id, bags[1].id]


def test_aging_latch_is_separate_from_a_manual_expedite(plant, flight):
    bag = check_in(plant, flight.id, 1)[0]
    plant.enqueue_recheck(bag.id)
    plant.promote_recheck(bag.id, RecheckPriority.PRIORITY)
    assert plant.recheck.entry(bag.id).aging_promoted is False
    plant.clock.advance(120)
    # Aging never re-lifts a bag that already left the normal band, however it
    # got there; the manual expedite keeps the aging latch unused.
    assert plant.reorder_recheck().promotions == []
    assert plant.recheck.entry(bag.id).aging_promoted is False


def test_every_priority_change_leaves_an_audit_entry(plant, flight):
    bags = check_in(plant, flight.id, 3)
    plant.enqueue_recheck(bags[0].id)
    plant.enqueue_recheck(bags[1].id)
    plant.expedite_recheck(bags[0].id, ExpeditePolicy.HEAD)
    plant.clock.advance(120)
    plant.enqueue_recheck(bags[2].id)
    reordered = plant.reorder_recheck()
    plant.process_recheck()
    assert plant.audit.verify() is True
    kinds = plant.audit.by_kind()
    assert kinds["recheck_enqueue"] == 3
    assert kinds["recheck_expedite"] == 1
    assert kinds["recheck_reorder"] == 1
    assert kinds["recheck_process"] == 1
    # The expedite audit names exactly who was starved by the head jump.
    expedited = next(
        entry for entry in plant.audit.entries() if entry.kind == "recheck_expedite"
    )
    assert expedited.subject == bags[0].id
    assert expedited.detail["starved"] == [bags[1].id]
    # The reorder audit lists every bag the aging pass lifted, entry by entry.
    reordered_entries = [
        entry for entry in plant.audit.entries() if entry.kind == "recheck_reorder"
    ]
    lifted = {
        promotion["bag_id"]
        for entry in reordered_entries
        for promotion in entry.detail.get("promotions", [])
    }
    assert lifted == {promotion.bag_id for promotion in reordered.promotions}
    # Each lifted bag also has its own promotion entry, so a bag's priority
    # history can be reviewed without scanning whole-queue reorder entries.
    aged_ids = {promotion.bag_id for promotion in reordered.promotions}
    for bag_id in aged_ids:
        subjects = plant.audit.for_subject(bag_id)
        promote_kinds = [entry.kind for entry in subjects if entry.kind == "recheck_promote"]
        assert promote_kinds == ["recheck_promote"]
        entry = subjects[-1]
        assert entry.detail["cause"] == "aging"
        assert entry.detail["priority"] == "priority"


def test_promotions_survive_restart_and_replay(plant, flight):
    bag = check_in(plant, flight.id, 1)[0]
    plant.enqueue_recheck(bag.id)
    plant.clock.advance(120)
    plant.reorder_recheck()
    plant.store.persist()
    reopened = Plant.build(Store.open(plant.store.path), clock=plant.clock)
    entry = reopened.recheck.entry(bag.id)
    assert entry.priority is RecheckPriority.PRIORITY
    assert entry.aging_promoted is True
    assert entry.effective_at == "2024-01-01T00:02:00Z"
    assert len(reopened.recheck.promotion_history()) == 1


# --------------------------------------------------------------------------
# Cross checking
# --------------------------------------------------------------------------


def test_view_joins_queue_order_with_cutoffs_and_latches(plant, flight):
    plant.book.set_cutoff(flight.id, "2024-01-01T00:10:00Z")
    bags = check_in(plant, flight.id, 2)
    plant.enqueue_recheck(bags[0].id)
    plant.enqueue_recheck(bags[1].id, RecheckPriority.PRIORITY, ExpeditePolicy.HEAD)
    plant.clock.advance(120)
    plant.reorder_recheck()
    rows = plant.recheck.view()
    # Aging re-asserts arrival order inside the priority band: the patient
    # bag that arrived first is served ahead of the expedite jump.
    assert [row.bag_id for row in rows] == [bags[0].id, bags[1].id]
    by_id = {row.bag_id: row for row in rows}
    assert by_id[bags[1].id].priority is RecheckPriority.PRIORITY
    assert by_id[bags[0].id].priority is RecheckPriority.PRIORITY
    assert by_id[bags[0].id].aging_promoted is True
    assert by_id[bags[0].id].seconds_to_cutoff == 480.0
    assert by_id[bags[0].id].cutoff_at == "2024-01-01T00:10:00Z"
    assert by_id[bags[0].id].position == 0


def test_policy_validates_thresholds():
    with pytest.raises(ValueError):
        AgingPolicy(aging_after_seconds=-1)
    with pytest.raises(ValueError):
        AgingPolicy(cutoff_within_seconds=-1)
