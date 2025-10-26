import os, time, requests, duckdb, pandas as pd
from dateutil import tz

API_KEY = os.getenv("API_FOOTBALL_KEY") 
BASE    = "https://v3.football.api-sports.io"
LEAGUE  = 78
SEASON  = 2025
DB_PATH = r"C:/Users/campo/Desktop/sports betting/warehouse.duckdb"

HEADERS = {"x-apisports-key": API_KEY, "x-rapidapi-host": "v3.football.api-sports.io"}

def get(endpoint, params):
    r = requests.get(f"{BASE}/{endpoint}", headers=HEADERS, params=params, timeout=30)
    r.raise_for_status()
    j = r.json()
    return j.get("response", [])

con = duckdb.connect(DB_PATH)

def get_debug(endpoint, params):
    # help understand why we have issues. typically, we saw that when trying 2025 data:
    # {'get': 'standings', 'parameters': {'league': '78', 'season': '2025'}, 'errors': {'plan': 'Free plans do not have access to this season, try from 2021 to 2023.'}, 'results': 0, 'paging': {'current': 1, 'total': 1}, 'response': []}
    r = requests.get(f"{BASE}/{endpoint}", headers=HEADERS, params=params, timeout=30)
    print("status:", r.status_code)
    j = r.json()
    print("errors:", j.get("errors"))
    print("results:", j.get("results"))
    return j



st_resp = get("standings", {"league": LEAGUE, "season": SEASON})
rows = []
for item in st_resp:
    for table in item.get("league", {}).get("standings", []):
        for t in table:
            team = t.get("team", {}) or {}
            rows.append({
                "team_id": team.get("id"),
                "season": SEASON,
                "rank": t.get("rank"),
                "points": t.get("points"),
                "gd": t.get("goalsDiff"),
                "form": t.get("form"),
                "played": t.get("all", {}).get("played"),
                "wins": t.get("all", {}).get("win"),
                "draws": t.get("all", {}).get("draw"),
                "losses": t.get("all", {}).get("lose"),
                "home_w": (t.get("home", {}) or {}).get("win"),
                "home_d": (t.get("home", {}) or {}).get("draw"),
                "home_l": (t.get("home", {}) or {}).get("lose"),
                "away_w": (t.get("away", {}) or {}).get("win"),
                "away_d": (t.get("away", {}) or {}).get("draw"),
                "away_l": (t.get("away", {}) or {}).get("lose"),
                "updated_ts": pd.Timestamp.utcnow()
            })
df_st = pd.DataFrame(rows)
con.execute("""
CREATE TABLE IF NOT EXISTS standings (
  team_id INTEGER, season INTEGER, rank INTEGER, points INTEGER, gd INTEGER, form VARCHAR,
  played INTEGER, wins INTEGER, draws INTEGER, losses INTEGER,
  home_w INTEGER, home_d INTEGER, home_l INTEGER, away_w INTEGER, away_d INTEGER, away_l INTEGER,
  updated_ts TIMESTAMP
)
""")
if not df_st.empty:
    con.execute("INSERT INTO standings SELECT * FROM df_st")

# --- INJURIES (“Sidelined/Injuries”) ---
inj = get("injuries", {"league": LEAGUE, "season": SEASON})
inj_rows = []
for e in inj:
    player = e.get("player", {}) or {}
    team   = e.get("team", {}) or {}
    fixture= e.get("fixture", {}) or {}
    info   = e.get("player", {}).get("reason") or e.get("reason")
    inj_rows.append({
        "player_id": player.get("id"),
        "team_id": team.get("id"),
        "season": SEASON,
        "fixture_id": (fixture or {}).get("id"),
        "reason": info,
        "start_date": pd.to_datetime(e.get("start"), errors="coerce"),
        "expected_return": pd.to_datetime(e.get("end"), errors="coerce"),
        "status": e.get("status"),
        "updated_ts": pd.Timestamp.utcnow()
    })
df_inj = pd.DataFrame(inj_rows)
con.execute("""
CREATE TABLE IF NOT EXISTS injuries (
  player_id INTEGER, team_id INTEGER, season INTEGER, fixture_id INTEGER,
  reason VARCHAR, start_date TIMESTAMP, expected_return TIMESTAMP, status VARCHAR,
  updated_ts TIMESTAMP
)
""")
if not df_inj.empty:
    con.execute("INSERT INTO injuries SELECT * FROM df_inj")

# --- PLAYER STATS (paged) ---
stats_rows = []
page = 1
while True:
    pl = get("players", {"league": LEAGUE, "season": SEASON, "page": page})
    if not pl: break
    for rec in pl:
        player = rec.get("player", {}) or {}
        stats  = (rec.get("statistics") or [{}])[0]
        team   = stats.get("team", {}) or {}
        games  = stats.get("games", {}) or {}
        shots  = stats.get("shots", {}) or {}
        goals  = stats.get("goals", {}) or {}
        passes = stats.get("passes", {}) or {}
        tackles= stats.get("tackles", {}) or {}
        duels  = stats.get("duels", {}) or {}
        cards  = stats.get("cards", {}) or {}
        stats_rows.append({
            "player_id": player.get("id"),
            "team_id": team.get("id"),
            "season": SEASON,
            "minutes": games.get("minutes"),
            "appearances": games.get("appearences") or games.get("appearances"),
            "lineups": games.get("lineups"),
            "rating": games.get("rating"),
            "shots_total": shots.get("total"), "shots_on": shots.get("on"),
            "goals": goals.get("total"), "assists": goals.get("assists"),
            "passes_total": passes.get("total"), "passes_key": passes.get("key"),
            "tackles": tackles.get("total"),
            "duels_total": duels.get("total"), "duels_won": duels.get("won"),
            "yellow": cards.get("yellow"), "red": cards.get("red"),
            "updated_ts": pd.Timestamp.utcnow()
        })
    page += 1
    time.sleep(0.25)  # stay friendly on free/pro tiers

df_ps = pd.DataFrame(stats_rows)
con.execute("""
CREATE TABLE IF NOT EXISTS player_stats (
  player_id INTEGER, team_id INTEGER, season INTEGER,
  minutes INTEGER, appearances INTEGER, lineups INTEGER, rating VARCHAR,
  shots_total INTEGER, shots_on INTEGER, goals INTEGER, assists INTEGER,
  passes_total INTEGER, passes_key INTEGER, tackles INTEGER,
  duels_total INTEGER, duels_won INTEGER, yellow INTEGER, red INTEGER,
  updated_ts TIMESTAMP
)
""")
if not df_ps.empty:
    con.execute("INSERT INTO player_stats SELECT * FROM df_ps")

con.close()
print({
    "standings_rows": len(df_st),
    "injuries_rows": len(df_inj),
    "player_stats_rows": len(df_ps)
})