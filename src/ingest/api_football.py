import os
import httpx
import pandas as pd
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

APIFOOTBALL_KEY = os.getenv("APIFOOTBALL_KEY")
APIFOOTBALL_BASE = "https://v3.football.api-sports.io"

HEADERS = {"x-apisports-key": APIFOOTBALL_KEY} if APIFOOTBALL_KEY else {}

def _get(endpoint: str, params: dict):
    if not APIFOOTBALL_KEY:
        raise RuntimeError("APIFOOTBALL_KEY not set. Put it in .env")
    with httpx.Client(base_url=APIFOOTBALL_BASE, headers=HEADERS, timeout=30) as client:
        r = client.get(endpoint, params=params)
        r.raise_for_status()
        return r.json()

def fetch_fixtures(league_id: int, season: int) -> pd.DataFrame:
    data = _get("/fixtures", {"league": league_id, "season": season})
    rows = []
    for item in data.get("response", []):
        fix = item.get("fixture", {})
        league = item.get("league", {})
        teams = item.get("teams", {})
        rows.append({
            "match_id": fix.get("id"),
            "league_id": league.get("id"),
            "season": league.get("season"),
            "kickoff_ts": datetime.fromisoformat(fix.get("date").replace("Z", "+00:00")) if fix.get("date") else None,
            "home_id": teams.get("home", {}).get("id"),
            "away_id": teams.get("away", {}).get("id"),
            "venue": (fix.get("venue") or {}).get("name"),
            "referee": fix.get("referee"),
            "status": (fix.get("status") or {}).get("short"),
        })
    return pd.DataFrame(rows)

def fetch_lineups(match_id: int) -> pd.DataFrame:
    data = _get("/fixtures/lineups", {"fixture": match_id})
    rows = []
    for team in data.get("response", []):
        team_id = team.get("team", {}).get("id")
        # starters
        for p in (team.get("startXI") or []):
            player = p.get("player", {})
            rows.append({
                "match_id": match_id,
                "team_id": team_id,
                "player_id": player.get("id"),
                "is_starter": True,
                "position": player.get("pos"),
                "minutes_expected": None,
            })
        # substitutes (optional)
        for p in (team.get("substitutes") or []):
            player = p.get("player", {})
            rows.append({
                "match_id": match_id,
                "team_id": team_id,
                "player_id": player.get("id"),
                "is_starter": False,
                "position": player.get("pos"),
                "minutes_expected": None,
            })
    return pd.DataFrame(rows)
