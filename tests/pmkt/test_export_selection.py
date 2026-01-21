from datetime import datetime, timedelta, timezone

from pmkt.cli import _filter_future_events


def test_filter_future_events() -> None:
    now = datetime.now(timezone.utc)
    past = (now - timedelta(days=365)).isoformat()
    future = (now + timedelta(days=365)).isoformat()
    events = [
        {"id": "1", "closed": True, "endDate": future},
        {"id": "2", "closed": False, "endDate": past},
        {"id": "3", "closed": False, "endDate": future},
        {"id": "4", "closed": False, "active": True},
    ]
    filtered = _filter_future_events(events)
    ids = {event["id"] for event in filtered}
    assert ids == {"3", "4"}
