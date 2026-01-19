import json
import pandas as pd

def test_read_events() -> None:
    """Test reading events from a file."""
    path = "./data/polymarket_events_10.json"

    with open(path, "r") as f:
        markets = json.load(f)  # list[dict]

    df = pd.json_normalize(markets)  # flattens nested dicts like events[0].id, etc.
    print(df.shape)
    df.head(3)