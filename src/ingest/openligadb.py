"""
OpenLigaDB client for Bundesliga fixtures/results.
Docs: https://www.openligadb.de/
Example: /getmatchdata/bl1/2023
"""
import os, re, httpx, pandas as pd, hashlib
from datetime import datetime, timezone
from dotenv import load_dotenv

load_dotenv()
OLDB_BASE   = "https://api.openligadb.de"
OLDB_LEAGUE = os.getenv("OLDB_LEAGUE", "bl1")
OLDB_SEASON = os.getenv("OLDB_SEASON", "2023")

def _norm_team(name: str) -> str:
    if not name: return ""
    s = name.lower()
    s = re.sub(r"[^a-z0-9]+", " ", s).strip()
    s = s.replace(" fc", "").replace(" sc", "")
    return re.sub(r"\s+", " ", s)

def _join_key(dt_iso: str, home: str, away: str) -> str:
    date_str = ""
    if dt_iso:
        try:
            date_str = datetime.fromisoformat(dt_iso.replace("Z","+00:00")).strftime("%Y-%m-%d")
        except Exception:
            # Some OLDB dumps use local time; try parsing without Z
            try:
                date_str = datetime.fromisoformat(dt_iso).strftime("%Y-%m-%d")
            except Exception:
                date_str = ""
    return f"{date_str}|{_norm_team(home)}|{_norm_team(away)}"

def _to_ts(dt_iso: str):
    if not dt_iso: return None
    for cand in (dt_iso, dt_iso.replace("Z","+00:00")):
        try:
            return datetime.fromisoformat(cand)
        except Exception:
            continue
    return None

def _hash64(s: str) -> int:
    # deterministic 64-bit from string (for PK when MatchID is missing)
    h = hashlib.md5(s.encode("utf-8")).hexdigest()[:16]
    return int(h, 16)

def fetch_fixtures_openligadb(league_key: str = OLDB_LEAGUE, season: str = OLDB_SEASON) -> pd.DataFrame:
    url = f"{OLDB_BASE}/getmatchdata/{league_key}/{season}"
    with httpx.Client(timeout=30) as client:
        r = client.get(url)
        r.raise_for_status()
        data = r.json()

    rows = []
    for m in data:
        # raw fields (be defensive)
        match_id = m.get("MatchID")
        kickoff  = m.get("MatchDateTimeUTC") or m.get("MatchDateTime")
        team1    = (m.get("Team1") or {}).get("TeamName")
        team2    = (m.get("Team2") or {}).get("TeamName")
        venue    = (m.get("Location") or {}).get("LocationCity")
        finished = bool(m.get("MatchIsFinished"))

        jkey = _join_key(kickoff, team1, team2)
        pk = int(match_id) if match_id is not None else _hash64(jkey or f"row-{len(rows)}")

        rows.append({
            "match_id": pk,
            "league_id": None,
            "season": int(season) if str(season).isdigit() else None,
            "kickoff_ts": _to_ts(kickoff),
            "home_id": None,
            "away_id": None,
            "venue": venue,
            "referee": None,
            "status": "FT" if finished else "NS",
            "join_key": jkey,
        })

    df = pd.DataFrame(rows)
    # sanity fix if join_key somehow empty: still ensure unique PKs
    df["match_id"] = df["match_id"].astype("int64")
    return df
