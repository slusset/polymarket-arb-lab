from pmkt_arb_lab.tracker import OpportunityTracker


def test_tracker_duration() -> None:
    tracker = OpportunityTracker(edge_threshold=0.0, poll_interval_s=5.0)
    tracker.update("t1", 1, "m1", 1.0, 0.1)
    tracker.update("t2", 2, "m1", 1.0, 0.1)
    tracker.update("t3", 3, "m1", 1.0, -0.1)
    events = list(tracker.events())
    assert len(events) == 1
    assert events[0].duration_s == 10.0
