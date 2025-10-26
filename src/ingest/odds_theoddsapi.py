"""
The Odds API v4 client for H2H odds snapshots.
Builds a fuzzy join key (date + normalized team names).
"""
import os, re, httpx, pandas as pd
import requests
from datetime import datetime, timezone
from typing import Optional
from dotenv import load_dotenv

load_dotenv()
THEODDSAPI_KEY = os.getenv("THEODDSAPI_KEY")
ODDS_BASE = "https://api.the-odds-api.com/v4"

def _norm_team(name: str) -> str:
    if not name: return ""
    s = name.lower()
    s = re.sub(r"[^a-z0-9]+", " ", s).strip()
    return re.sub(r"\s+", " ", s.replace(" fc","").replace(" sc",""))

def _join_key(commence_iso: str, home: str, away: str) -> str:
    try:
        dt = datetime.fromisoformat(commence_iso.replace("Z","+00:00")).strftime("%Y-%m-%d")
    except Exception:
        dt = ""
    return f"{dt}|{_norm_team(home)}|{_norm_team(away)}"

def _get(path: str, params: dict):
    if not THEODDSAPI_KEY:
        raise RuntimeError("THEODDSAPI_KEY not set. Put it in .env")
    with httpx.Client(base_url=ODDS_BASE, timeout=30) as client:
        r = client.get(path, params={"apiKey": THEODDSAPI_KEY, **params})
        try:
            r.raise_for_status()
        except httpx.HTTPStatusError as e:
            print("TheOddsAPI error:", e.response.status_code, e.response.text)
            raise
        return r.json()

def _api_key() -> str:
    k = os.getenv("THE_ODDS_API_KEY") or os.getenv("ODDS_API_KEY")
    if not k:
        raise RuntimeError("Set env var THE_ODDS_API_KEY with your The Odds API key")
    return k

def fetch_h2h(
    sport_key: str,
    regions: str = "eu",           # “eu” usually covers Bundesliga books; add “us,uk,eu” if you want more
    markets: str = "h2h",          # 3-way match result
    odds_format: str = "decimal",
    date_format: str = "iso",
    timeout: float = 15.0,
) -> list[dict]:
    """
    Returns a list of events (dicts) in The Odds API v4 shape.
    Each event has: id, sport_key, sport_title, commence_time, home_team, away_team, bookmakers[…]
    """
    key = _api_key()
    url = f"{ODDS_BASE}/sports/{sport_key}/odds"
    params = {
        "apiKey": key,
        "regions": regions,
        "markets": markets,
        "oddsFormat": odds_format,
        "dateFormat": date_format,
    }
    r = requests.get(url, params=params, timeout=timeout)
    if r.status_code != 200:
        # Keep it graceful so your pipeline doesn’t crash without context
        raise RuntimeError(f"The Odds API error {r.status_code}: {r.text}")
    data = r.json()
    # Always return a list
    return data if isinstance(data, list) else []


def fetch_h2h_odds_snapshot(
    sport_key: str,
    regions: str = "eu,uk,us",
    markets: str = "h2h",
    date_from: Optional[str] = None,
    date_to: Optional[str] = None,
) -> pd.DataFrame:
    events = _get(f"/sports/{sport_key}/odds", {
        "regions": regions,
        "markets": markets,
        "dateFormat": "iso",
        "oddsFormat": "decimal",
    })
    rows, pulled_ts = [], datetime.now(timezone.utc)
    for ev in events:
        event_id = ev.get("id")
        home     = ev.get("home_team")
        away     = ev.get("away_team")
        commence = ev.get("commence_time")
        jkey     = _join_key(commence, home, away)
        for book in ev.get("bookmakers", []):
            book_key = book.get("key")
            for mk in book.get("markets", []):
                if mk.get("key") != "h2h":
                    continue
                for o in mk.get("outcomes", []):
                    name, price = o.get("name"), o.get("price")
                    norm = None
                    if name in {"home","Home","1",home}: norm = "home"
                    elif name in {"draw","Draw","X"}:  norm = "draw"
                    elif name in {"away","Away","2",away}: norm = "away"
                    else:
                        if name and home and name.lower()==home.lower(): norm="home"
                        if name and away and name.lower()==away.lower(): norm="away"
                    if norm is None: continue
                    rows.append({
                        "event_id": str(event_id),
                        "match_id": None,
                        "book": book_key,
                        "market": "h2h",
                        "outcome": norm,
                        "price_dec": float(price) if price is not None else None,
                        "ts": pulled_ts,
                        "event_join_key": jkey,
                        "is_closing": False,
                    })
    return pd.DataFrame(rows)
