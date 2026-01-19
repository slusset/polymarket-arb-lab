import json

import pandas as pd


def decode_json_list_maybe(x):
    if isinstance(x, list):
        return x
    if isinstance(x, str):
        s = x.strip()
        if s.startswith("[") and s.endswith("]"):
            try:
                return json.loads(s)
            except Exception:
                return None
    return None


def is_two_outcome(row):
    outs = row.get("outcomes")
    toks = row.get("clobTokenIds")
    return (
        isinstance(outs, list) and len(outs) == 2 and
        isinstance(toks, list) and len(toks) == 2
    )


def outcome_token_map(row):
    outs = row.get("outcomes")
    toks = row.get("clobTokenIds")
    if not (isinstance(outs, list) and isinstance(toks, list) and len(outs) == len(toks) == 2):
        return None
    return {str(outs[i]): str(toks[i]) for i in range(2)}


path = "data/polymarket_events_10.json"

with open(path, "r") as f:
    events = json.load(f)          # list[dict]

mk = pd.json_normalize(
    events,
    record_path="markets",
    meta=["id", "slug", "title", "ticker", "active", "closed", "startDate", "endDate"],
    meta_prefix="event_",
    errors="ignore"
)

for col in ["outcomes", "clobTokenIds", "outcomePrices"]:
    if col in mk.columns:
        mk[col] = mk[col].apply(decode_json_list_maybe)

print(type(mk))
print(mk.shape if hasattr(mk, "shape") else "no shape")
print(mk.columns[:20] if hasattr(mk, "columns") else "no columns")


# 1) Eligible markets (tradable + orderbook)
eligible = (
    (mk["closed"] == False) &
    (mk["enableOrderBook"] == True) &
    (mk["acceptingOrders"].fillna(True) == True)   # tolerate missing/NaN
)

open_clob_markets = mk.loc[eligible].copy()

# 2) Binary-eligible markets (exactly 2 outcomes + 2 token ids)
binary_markets = open_clob_markets.loc[
    open_clob_markets.apply(is_two_outcome, axis=1)
].copy()

binary_markets["outcome_token_map"] = binary_markets.apply(outcome_token_map, axis=1)

print("Total markets rows:", len(mk))
print("Binary eligible:", len(binary_markets))

excluded = mk.loc[~eligible, ["id","question","enableOrderBook","acceptingOrders","closed","outcomes"]]
print("Excluded rows:", len(excluded))
print(binary_markets.to_string(index=False))


# mk["has_two_outcomes"] = mk["outcomes"].apply(lambda x: isinstance(x, list) and len(x)==2)
# mk["has_two_tokens"] = mk["clobTokenIds"].apply(lambda x: isinstance(x, list) and len(x)==2)
#
# mk["fail_closed"] = mk["closed"] != False
# mk["fail_enableOrderBook"] = mk["enableOrderBook"] != True
# mk["fail_acceptingOrders"] = mk["acceptingOrders"] != True
# mk["fail_two_outcome"] = ~mk["has_two_outcomes"]
# mk["fail_two_tokens"] = ~mk["has_two_tokens"]
#
# print(mk[["fail_closed","fail_enableOrderBook","fail_acceptingOrders","fail_two_outcome","fail_two_tokens"]].sum())

#
# print(mk[["id","enableOrderBook","acceptingOrders","closed","has_two_outcomes","has_two_tokens"]]
#       .value_counts().head(20))

# Show the top 20 by recent CLOB volume
#cols_show = ["id", "question", "endDate", "volume24hrClob", "liquidityClob", "outcome_token_map"]
#print(binary.sort_values("volume24hrClob", ascending=False)[cols_show].head(20).to_string(index=False))