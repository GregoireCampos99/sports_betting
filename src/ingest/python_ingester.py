import os, time, math, requests, pandas as pd, duckdb
from datetime import datetime, timezone

API_KEY = os.getenv("API_FOOTBALL_KEY")
BASE    = "https://v3.football.api-sports.io"
HDRS    = {"x-apisports-key": API_KEY}
DB_PATH = r"C:/Users/campo/Desktop/sports betting/warehouse.duckdb"

COUNTRIES = ["England","France","Germany","Italy","Spain"]
LEAGUE_NAMES = {
    "England": "Premier League",
    "France":  "Ligue 1",
    "Germany": "Bundesliga",
    "Italy":   "Serie A",
    "Spain":   "La Liga",
}
SEASONS = list(range(2020, 2026))  # inclusive 2020..2025
SOURCE = "api-football"

SLEEP = 0.25  # be gentle even on PRO
RETRIES = 3

def api_get(endpoint, params, retries=RETRIES):
    for i in range(retries):
        r = requests.get(f"{BASE}/{endpoint}", headers=HDRS, params=params, timeout=30)
        if r.status_code >= 500:
            time.sleep(1 + i)
            continue
        r.raise_for_status()
        j = r.json()
        if j.get("errors"):
            # bubble up plan/coverage/quota errors explicitly
            raise RuntimeError(f"{endpoint} error: {j['errors']} params={params}")
        return j
    raise RuntimeError(f"{endpoint} failed after retries params={params}")

def discover_league_ids():
    """Return {country: league_id} for target top leagues via /leagues."""
    out = {}
    for c in COUNTRIES:
        j = api_get("leagues", {"country": c})
        # pick the league whose name matches our target (case-insensitive)
        target = LEAGUE_NAMES[c].lower()
        cand = []
        for item in j.get("response", []):
            lg = item.get("league", {}) or {}
            name = (lg.get("name") or "").lower()
            if target in name:
                cand.append(lg.get("id"))
        if not cand:
            # fallback: choose highest coverage with standings
            best, best_cov = None, -1
            for item in j.get("response", []):
                cov = (((item.get("seasons") or [])[-1] or {}).get("coverage") or {}).get("standings")
                if cov:
                    lgid = (item.get("league") or {}).get("id")
                    best, best_cov = lgid, 1
            if best:
                out[c] = best
            else:
                raise RuntimeError(f"No league id found for {c}")
        else:
            out[c] = cand[0]
        time.sleep(SLEEP)
    return out

def upsert_df(con, table, df, key_cols):
    if df.empty: return 0
    tmp = f"tmp_{table}_{int(time.time()*1000)}"
    con.register("df_src", df)
    con.execute(f"CREATE TEMP TABLE {tmp} AS SELECT * FROM df_src")
    on_clause = " AND ".join([f"t.{k}=s.{k}" for k in key_cols])
    set_cols = [c for c in df.columns if c not in key_cols]
    set_clause = ", ".join([f"{c}=excluded.{c}" for c in set_cols])

    # DuckDB 1.1+ supports MERGE; use INSERT ON CONFLICT for simplicity
    cols = ", ".join(df.columns)
    placeholders = ", ".join([f"s.{c}" for c in df.columns])
    pk = ", ".join(key_cols)

    # ensure PK exists
    # (if not created earlier, you can ALTER TABLE to add a PK)

    con.execute(f"""
    INSERT INTO {table} ({cols})
    SELECT {placeholders} FROM {tmp} s
    ON CONFLICT ({pk}) DO UPDATE SET {set_clause}
    """)
    con.execute(f"DROP TABLE {tmp}")
    return len(df)

def ingest_standings(con, league_id, season):
    j = api_get("standings", {"league": league_id, "season": season})
    rows = []
    for item in j.get("response", []):
        league = item.get("league", {}) or {}
        for table in league.get("standings", []):
            for t in table:
                team = t.get("team", {}) or {}
                rows.append({
                    "league_id": league_id, "season": season,
                    "team_id": team.get("id"), "team_name": team.get("name"),
                    "rank": t.get("rank"),
                    "played": t.get("all",{}).get("played"),
                    "wins": t.get("all",{}).get("win"),
                    "draws": t.get("all",{}).get("draw"),
                    "losses": t.get("all",{}).get("lose"),
                    "points": t.get("points"),
                    "gf": t.get("all",{}).get("goals",{}).get("for"),
                    "ga": t.get("all",{}).get("goals",{}).get("against"),
                    "gd": t.get("goalsDiff"),
                    "form": t.get("form"),
                    "source": SOURCE, "updated_ts": pd.Timestamp.utcnow()
                })
    df = pd.DataFrame(rows)
    return upsert_df(con, "standings", df, ["league_id","season","team_id"])

def ingest_injuries(con, league_id, season):
    j = api_get("injuries", {"league": league_id, "season": season})
    rows = []
    for e in j.get("response", []):
        player = e.get("player", {}) or {}
        team   = e.get("team", {}) or {}
        fixture= e.get("fixture", {}) or {}
        rows.append({
            "league_id": league_id, "season": season,
            "team_id": team.get("id"),
            "player_id": player.get("id"),
            "player_name": player.get("name"),
            "reason": e.get("reason") or (player.get("reason") if isinstance(player.get("reason"), str) else None),
            "status": e.get("status"),
            "start_date": pd.to_datetime(e.get("start"), errors="coerce"),
            "expected_return": pd.to_datetime(e.get("end"), errors="coerce"),
            "fixture_id": fixture.get("id"),
            "source": SOURCE, "updated_ts": pd.Timestamp.utcnow()
        })
    df = pd.DataFrame(rows)
    return upsert_df(con, "injuries", df, ["league_id","season","team_id","player_id","start_date"])

def ingest_player_stats(con, league_id, season):
    total = 0
    page = 1
    while True:
        j = api_get("players", {"league": league_id, "season": season, "page": page})
        resp = j.get("response", [])
        if not resp: break
        rows = []
        for rec in resp:
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
            rows.append({
                "league_id": league_id, "season": season,
                "team_id": team.get("id"), "player_id": player.get("id"),
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
                "source": SOURCE, "updated_ts": pd.Timestamp.utcnow()
            })
        df = pd.DataFrame(rows)
        total += upsert_df(con, "player_stats", df, ["league_id","season","player_id","team_id"])
        page += 1
        time.sleep(SLEEP)
    return total

def ensure_schema(con):
    con.execute("""
    CREATE TABLE IF NOT EXISTS standings (
      league_id INTEGER, season INTEGER, team_id INTEGER, team_name VARCHAR,
      rank INTEGER, played INTEGER, wins INTEGER, draws INTEGER, losses INTEGER,
      points INTEGER, gf INTEGER, ga INTEGER, gd INTEGER, form VARCHAR,
      source VARCHAR, updated_ts TIMESTAMP,
      PRIMARY KEY (league_id, season, team_id)
    );
    """)
    con.execute("""
    CREATE TABLE IF NOT EXISTS injuries (
      league_id INTEGER, season INTEGER, team_id INTEGER, player_id INTEGER,
      player_name VARCHAR, reason VARCHAR, status VARCHAR,
      start_date TIMESTAMP, expected_return TIMESTAMP, fixture_id INTEGER,
      source VARCHAR, updated_ts TIMESTAMP,
      PRIMARY KEY (league_id, season, team_id, player_id, start_date)
    );
    """)
    con.execute("""
    CREATE TABLE IF NOT EXISTS player_stats (
      league_id INTEGER, season INTEGER, team_id INTEGER, player_id INTEGER,
      minutes INTEGER, appearances INTEGER, lineups INTEGER, rating VARCHAR,
      shots_total INTEGER, shots_on INTEGER, goals INTEGER, assists INTEGER,
      passes_total INTEGER, passes_key INTEGER, tackles INTEGER,
      duels_total INTEGER, duels_won INTEGER, yellow INTEGER, red INTEGER,
      source VARCHAR, updated_ts TIMESTAMP,
      PRIMARY KEY (league_id, season, player_id, team_id)
    );
    """)

def main():
    con = duckdb.connect(DB_PATH)
    ensure_schema(con)  # <-- add this
    leagues = discover_league_ids()
    con = duckdb.connect(DB_PATH)
    # ensure tables exist (DDL above)
    leagues = discover_league_ids()  # {'England': 39, 'France': 61, 'Germany': 78, 'Italy': 135, 'Spain': 140} typically
    print("Leagues:", leagues)

    for country, league_id in leagues.items():
        for season in SEASONS:
            try:
                s = ingest_standings(con, league_id, season)
                print(f"[standings] {country} {season}: upserted {s}")
            except Exception as e:
                print(f"[standings] {country} {season}: {e}")

            try:
                inj = ingest_injuries(con, league_id, season)
                print(f"[injuries ] {country} {season}: upserted {inj}")
            except Exception as e:
                print(f"[injuries ] {country} {season}: {e}")

            try:
                ps = ingest_player_stats(con, league_id, season)
                print(f"[plyrstats] {country} {season}: upserted {ps}")
            except Exception as e:
                print(f"[plyrstats] {country} {season}: {e}")

            time.sleep(SLEEP)

    con.close()

if __name__ == "__main__":
    main()





