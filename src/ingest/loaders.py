from __future__ import annotations
import requests
import pandas as pd
# from src.warehouse.io import Warehouse
from datetime import timezone
from typing import Dict, Any, List
from .openligadb import fetch_fixtures_openligadb
from .odds_theoddsapi import fetch_h2h_odds_snapshot, fetch_h2h
from ..warehouse.io import DB
import os
from dotenv import load_dotenv

load_dotenv()
SPORT_KEY = os.getenv("ODDS_SPORT_KEY", "soccer_germany_bundesliga")
NUMERIC_COLS = ["price_home", "price_draw", "price_away"]
INT_COLS = ["season"]
TS_COLS = ["last_update", "commence_time"]

def _coerce_odds_dtypes(df):
    if df is None or df.empty: return df
    import pandas as pd
    for c in TS_COLS:
        if c in df.columns: df[c] = pd.to_datetime(df[c], utc=True, errors="coerce")
    for c in NUMERIC_COLS:
        if c in df.columns: df[c] = pd.to_numeric(df[c], errors="coerce")
    for c in INT_COLS:
        if c in df.columns: df[c] = pd.to_numeric(df[c], errors="coerce").astype("Int64")
    return df

def _coerce_odds_dtypes(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return df
    # Timestamps (ensure tz-aware)
    for c in TS_COLS:
        if c in df.columns:
            df[c] = pd.to_datetime(df[c], utc=True, errors="coerce")
    # Floats
    for c in NUMERIC_COLS:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")
    # Integers
    for c in INT_COLS:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce").astype("Int64")
    return df

def ingest_odds_snapshot(sport_key) -> dict:
    payload = fetch_h2h(sport_key=sport_key)
    odds = build_odds_df_from_theoddsapi(payload, league_key="soccer_germany_bundesliga")
    odds = _coerce_odds_dtypes(odds)
    # sanity
    required = {"event_id", "market", "bookmaker", "commence_time", "home_team", "away_team"}
    missing = required - set(odds.columns)
    if missing:
        raise ValueError(f"odds df missing required cols: {missing}")
    db = DB()
    n = db.insert_df("odds", odds)
    return {"odds_rows": int(n)}

# def ingest_matches_openligadb():
#     db = DB()
#     fixtures = fetch_fixtures_openligadb()
#     try:
#         print("fixtures.shape:", fixtures.shape)
#         print("fixtures.columns:", list(fixtures.columns))
#         print(fixtures.head(3))
#     except Exception:
#         pass
#     if fixtures is None or getattr(fixtures, "empty", True):
#         print("No fixtures returned from OpenLigaDB. Check league/season.")
#         return {"matches": 0}
#     n1 = db.insert_df("matches", fixtures)
#     return {"matches": n1}

# src/ingest/loaders.py
import requests
import pandas as pd

def ingest_matches_openligadb(league="bl1", season="2023"):
    """Fetch fixtures from OpenLigaDB and insert into 'matches' table.
       Returns: {"matches": <rows_inserted>, "dropped": <rows_dropped>}
    """
    def _fetch(league, season_key):
        url = f"https://api.openligadb.de/getmatchdata/{league}/{season_key}"
        r = requests.get(url, timeout=20, headers={"User-Agent": "sports-betting/0.1"})
        r.raise_for_status()
        return r.json()

    def _lower_keys(d):
        return {k.lower(): v for k, v in d.items()} if isinstance(d, dict) else {}

    # OpenLigaDB BL1 wants start year like "2023"
    data = _fetch(league, str(season))
    if not isinstance(data, list) or not data:
        raise RuntimeError(f"No data for league='{league}', season='{season}'")

    rows = []
    dropped_no_id = 0
    dropped_no_teams = 0

    for m in data:
        ml = _lower_keys(m)

        match_id = ml.get("matchid")
        if match_id in (None, ""):
            dropped_no_id += 1
            continue

        # teams
        t1 = _lower_keys(ml.get("team1") or {})
        t2 = _lower_keys(ml.get("team2") or {})
        home_id = t1.get("teamid")
        away_id = t2.get("teamid")
        if home_id in (None, "") or away_id in (None, ""):
            dropped_no_teams += 1
            continue

        # kickoff
        kickoff = ml.get("matchdatetimeutc") or ml.get("matchdatetime")
        kickoff_ts = pd.to_datetime(kickoff, utc=True, errors="coerce")

        # venue (best-effort)
        loc = _lower_keys(ml.get("location") or {})
        stadium = _lower_keys(loc.get("stadium") or {})
        venue = stadium.get("name") or loc.get("locationstadium")

        # referee usually not present in BL1 feed; leave None
        referee = None

        # status
        is_finished = bool(ml.get("matchisfinished"))
        status = "FT" if is_finished else "NS"
        if not is_finished and kickoff_ts is not pd.NaT:
            try:
                if pd.Timestamp.utcnow() >= kickoff_ts:
                    status = "LIVE"
            except Exception:
                pass

        # numeric season (start year)
        try:
            season_num = int(str(season)[:4])
        except Exception:
            season_num = None

        rows.append({
            "match_id":   match_id,
            "league_id":  ml.get("leagueid"),   # optional, can be None
            "season":     season_num,
            "kickoff_ts": kickoff_ts,
            "home_id":    home_id,
            "away_id":    away_id,
            "venue":      venue,
            "referee":    referee,
            "status":     status,
            "join_key":   f"{home_id}||{away_id}",
        })

    df = pd.DataFrame(rows, columns=[
        "match_id","league_id","season","kickoff_ts","home_id","away_id",
        "venue","referee","status","join_key"
    ])

    total = len(data)
    valid = len(df)
    print(f"[openligadb] total={total} valid={valid} dropped_no_id={dropped_no_id} dropped_no_teams={dropped_no_teams}")

    if df.empty:
        raise ValueError("All rows dropped; unexpected API shape. Check probe output again.")

    # Optional: quick peek
    # print(df.head(3)); print(df.isna().mean())

    db = DB()
    n_inserted = db.insert_df("matches", df)
    return {"matches": int(n_inserted), "dropped": int(dropped_no_id + dropped_no_teams)}

def _normalize_team(s: str) -> str:
    # Make sure these names match your fixtures' naming
    # Add mappings like {"Bayern Munich":"FC Bayern München"} if needed.
    return (s or "").strip()

def _mk_join_key(commence_iso: str, home: str, away: str) -> str:
    # date-only (UTC) + normalized team names
    dt = pd.to_datetime(commence_iso, utc=True)
    d = dt.strftime("%Y-%m-%d")
    return f"{d}|{_normalize_team(home)}|{_normalize_team(away)}"

def build_odds_df_from_theoddsapi(payload: List[Dict[str, Any]], league_key: str | None = None) -> pd.DataFrame:
    rows = []
    for ev in payload:
        event_id      = ev.get("id")
        sport_key     = ev.get("sport_key")
        sport_title   = ev.get("sport_title")
        commence_time = ev.get("commence_time")   # ISO8601
        home_team     = ev.get("home_team")
        away_team     = ev.get("away_team")
        join_key      = _mk_join_key(commence_time, home_team, away_team)

        # Each bookmaker…
        for bm in ev.get("bookmakers", []):
            bookmaker   = bm.get("title")
            last_update = bm.get("last_update")   # ISO8601
            # Find H2H market (3-way) — usually key == "h2h"
            for market in bm.get("markets", []):
                mkey = market.get("key")  # "h2h"
                if mkey != "h2h":
                    continue

                # Normalize outcomes to home/draw/away
                price_home, price_draw, price_away = None, None, None
                for out in market.get("outcomes", []):
                    name  = out.get("name")   # "Home" / "Away" / "Draw" (provider-dependent)
                    price = out.get("price")
                    # Be defensive about naming; map by matching team names too
                    if name in ("Home", home_team):
                        price_home = price
                    elif name in ("Away", away_team):
                        price_away = price
                    elif name in ("Draw", "Tie", "X"):
                        price_draw = price

                rows.append({
                    "event_id": event_id,
                    "sport_key": sport_key,
                    "sport_title": sport_title,
                    "league_key": league_key,
                    "market": mkey,
                    "bookmaker": bookmaker,
                    "last_update": pd.to_datetime(last_update, utc=True),
                    "commence_time": pd.to_datetime(commence_time, utc=True),
                    "home_team": home_team,
                    "away_team": away_team,
                    "price_home": float(price_home) if price_home is not None else None,
                    "price_draw": float(price_draw) if price_draw is not None else None,
                    "price_away": float(price_away) if price_away is not None else None,
                    "join_key": join_key,
                })

    df = pd.DataFrame(rows)
    # (Optional) add season if you need it for joins
    if not df.empty:
        df["season"] = df["commence_time"].dt.year.astype("Int64")
    return df


####### VERSION EPL #######

# # src/ingest/loaders.py
# from .api_football import fetch_fixtures, fetch_lineups
# from .odds_theoddsapi import fetch_h2h_odds_snapshot
# from ..warehouse.io import DB
# import os
# from dotenv import load_dotenv

# load_dotenv()

# LEAGUE_ID = int(os.getenv("LEAGUE_ID", "39"))
# SEASON = int(os.getenv("SEASON", "2024"))
# SPORT_KEY = os.getenv("ODDS_SPORT_KEY", "soccer_epl")

# def ingest_fixtures_and_lineups():
#     db = DB()
#     fixtures = fetch_fixtures(LEAGUE_ID, SEASON)

#     # DEBUG: show what we got
#     try:
#         print("fixtures.shape:", fixtures.shape)
#         print("fixtures.columns:", list(fixtures.columns))
#         print(fixtures.head(3))
#     except Exception:
#         pass

#     if fixtures is None or getattr(fixtures, "empty", True):
#         print("No fixtures returned. Check API key/league/season or quota.")
#         return {"matches": 0, "lineups": 0}

#     # Ensure required columns exist
#     if "match_id" not in fixtures.columns:
#         raise RuntimeError(
#             f"'match_id' column missing in fixtures. Columns present: {list(fixtures.columns)}"
#         )

#     n1 = db.insert_df("matches", fixtures)

#     mids = (
#         fixtures["match_id"]
#         .dropna()
#         .astype("int64", errors="ignore")
#         .unique()
#         .tolist()
#     )

#     total_lineups = 0
#     for mid in mids:
#         try:
#             lu = fetch_lineups(int(mid))
#             total_lineups += db.insert_df("lineups", lu)
#         except Exception as e:
#             print(f"[warn] lineups for match {mid} failed: {e}")

#     return {"matches": n1, "lineups": total_lineups}

# def ingest_odds_snapshot():
#     db = DB()
#     odds = fetch_h2h_odds_snapshot(SPORT_KEY)
#     if odds is None or getattr(odds, "empty", True):
#         print("No odds snapshot returned (maybe no upcoming events or bad SPORT_KEY).")
#         return {"odds": 0}
#     n = db.insert_df("odds", odds)
#     return {"odds": n}
