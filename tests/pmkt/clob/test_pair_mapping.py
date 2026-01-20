import json

from pmkt.clob.paired_recorder import _parse_outcome_tokens, _resolve_outcome_pair


def test_outcome_mapping_up_down() -> None:
    row = {
        "tokens": json.dumps(
            [
                {"outcome": "Up", "token_id": "token-up"},
                {"outcome": "Down", "token_id": "token-down"},
            ]
        )
    }
    outcome_tokens = _parse_outcome_tokens(row)
    outcome_a, outcome_b, token_a, token_b = _resolve_outcome_pair(outcome_tokens)
    assert (outcome_a, outcome_b) == ("Up", "Down")
    assert (token_a, token_b) == ("token-up", "token-down")


def test_outcome_mapping_yes_no_canonical() -> None:
    row = {
        "tokens": json.dumps(
            [
                {"outcome": "No", "token_id": "token-no"},
                {"outcome": "Yes", "token_id": "token-yes"},
            ]
        )
    }
    outcome_tokens = _parse_outcome_tokens(row)
    outcome_a, outcome_b, token_a, token_b = _resolve_outcome_pair(outcome_tokens)
    assert (outcome_a, outcome_b) == ("Yes", "No")
    assert (token_a, token_b) == ("token-yes", "token-no")


def test_outcome_mapping_unknown_labels_sorted() -> None:
    row = {
        "tokens": json.dumps(
            [
                {"outcome": "Republican", "token_id": "token-rep"},
                {"outcome": "Democrat", "token_id": "token-dem"},
            ]
        )
    }
    outcome_tokens = _parse_outcome_tokens(row)
    outcome_a, outcome_b, token_a, token_b = _resolve_outcome_pair(outcome_tokens)
    assert (outcome_a, outcome_b) == ("Democrat", "Republican")
    assert (token_a, token_b) == ("token-dem", "token-rep")
