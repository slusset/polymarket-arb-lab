import sqlite3

from pmkt_arb_lab.storage import Storage


def test_execute_with_retry_eventually_succeeds(tmp_path, monkeypatch) -> None:
    storage = Storage(str(tmp_path / "test.db"))
    monkeypatch.setattr("time.sleep", lambda _: None)
    calls = {"count": 0}

    def action() -> None:
        calls["count"] += 1
        if calls["count"] < 3:
            raise sqlite3.OperationalError("database is locked")

    ok = storage._execute_with_retry("test", action)
    assert ok is True
    assert calls["count"] == 3
    storage.close()


def test_execute_with_retry_gives_up(tmp_path, monkeypatch) -> None:
    storage = Storage(str(tmp_path / "test.db"))
    monkeypatch.setattr("time.sleep", lambda _: None)

    def action() -> None:
        raise sqlite3.OperationalError("database is locked")

    ok = storage._execute_with_retry("test", action)
    assert ok is False
    storage.close()
