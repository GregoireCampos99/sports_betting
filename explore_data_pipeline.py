import os, time, requests, certifi, duckdb, pandas as pd
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

DB_PATH = r"C:/Users/campo/Desktop/sports betting/warehouse.duckdb"
BASE = "https://v3.football.api-sports.io"
HDRS = {"x-apisports-key": os.getenv("API_FOOTBALL_KEY")}
SLEEP = 0.15

session = requests.Session()
session.headers.update(HDRS)
session.verify = certifi.where()
session.mount("https://", HTTPAdapter(max_retries=Retry(
    total=5, backoff_factor=0.4, status_forcelist=[429,500,502,503,504],
    allowed_methods=["GET"]
)))

def ensure_schema(con):
    # === Dimension tables ===
    con.execute("""
    CREATE TABLE IF NOT EXISTS dim_league (
        league_id INTEGER PRIMARY KEY,
        league_name VARCHAR,
        country_name VARCHAR,
        country_code VARCHAR
    );
    """)

    con.execute("""
    CREATE TABLE IF NOT EXISTS dim_team (
        team_id INTEGER PRIMARY KEY,
        team_name VARCHAR,
        country_name VARCHAR
    );
    """)

    con.execute("""
    CREATE TABLE IF NOT EXISTS dim_player (
        player_id INTEGER PRIMARY KEY,
        player_name VARCHAR,
        firstname VARCHAR,
        lastname VARCHAR,
        nationality VARCHAR,
        birth_date DATE,
        height VARCHAR,
        weight VARCHAR
    );
    """)

    # === Fact: Fixtures ===
    con.execute("""
    CREATE TABLE IF NOT EXISTS fact_fixtures (
        fixture_id INTEGER PRIMARY KEY,
        league_id INTEGER,
        season INTEGER,
        round VARCHAR,
        date_utc TIMESTAMP,
        venue_id INTEGER,
        venue_name VARCHAR,
        status_short VARCHAR,
        home_team_id INTEGER,
        away_team_id INTEGER,
        home_goals INTEGER,
        away_goals INTEGER,
        referee VARCHAR,
        updated_ts TIMESTAMP
    );
    """)

    # === Fact: Player Stats per Fixture ===
    con.execute("""
    CREATE TABLE IF NOT EXISTS fact_player_stats_match (
        fixture_id INTEGER,
        league_id INTEGER,
        season INTEGER,
        team_id INTEGER,
        player_id INTEGER,
        player_name VARCHAR,
        team_name VARCHAR,
        position VARCHAR,
        number INTEGER,
        is_captain BOOLEAN,
        minutes INTEGER,
        rating VARCHAR,
        shots_total INTEGER,
        shots_on INTEGER,
        goals INTEGER,
        assists INTEGER,
        saves INTEGER,
        passes_total INTEGER,
        passes_key INTEGER,
        passes_accuracy INTEGER,
        tackles INTEGER,
        interceptions INTEGER,
        blocks INTEGER,
        duels_total INTEGER,
        duels_won INTEGER,
        dribbles_attempts INTEGER,
        dribbles_success INTEGER,
        fouls_committed INTEGER,
        fouls_drawn INTEGER,
        yellow INTEGER,
        red INTEGER,
        offsides INTEGER,
        updated_ts TIMESTAMP,
        PRIMARY KEY (fixture_id, player_id, team_id)
    );
    """)

    # === Fact: Standings snapshots ===
    con.execute("""
    CREATE TABLE IF NOT EXISTS fact_standings_snapshot (
        league_id INTEGER,
        season INTEGER,
        matchday INTEGER,
        team_id INTEGER,
        rank INTEGER,
        points INTEGER,
        played INTEGER,
        wins INTEGER,
        draws INTEGER,
        losses INTEGER,
        gf INTEGER,
        ga INTEGER,
        gd INTEGER,
        form VARCHAR,
        snapshot_ts TIMESTAMP,
        PRIMARY KEY (league_id, season, matchday, team_id)
    );
    """)

    # === Fact: Injuries ===
    con.execute("""
    CREATE TABLE IF NOT EXISTS fact_injuries (
        league_id INTEGER,
        season INTEGER,
        team_id INTEGER,
        player_id INTEGER,
        player_name VARCHAR,
        status VARCHAR,
        reason VARCHAR,
        start_date DATE,
        expected_return DATE,
        fixture_id INTEGER,
        updated_ts TIMESTAMP,
        PRIMARY KEY (league_id, season, team_id, player_id, updated_ts)
    );
    """)
    # === Fact: Odds ===
    con.execute("""
    CREATE TABLE IF NOT EXISTS fact_odds (
        fixture_id INTEGER,
        league_id INTEGER,
        season INTEGER,
        bookmaker_id INTEGER,
        bookmaker_name VARCHAR,
        market_key VARCHAR,
        selection VARCHAR,
        value DOUBLE,
        last_update TIMESTAMP,
        updated_ts TIMESTAMP,
        PRIMARY KEY (fixture_id, bookmaker_id, market_key, selection)
    );
    """)

def api_get(endpoint, params):
    r = session.get(f"{BASE}/{endpoint}", params=params, timeout=30)
    r.raise_for_status()
    j = r.json()
    if j.get("errors"):
        raise RuntimeError(f"{endpoint} error: {j['errors']} params={params}")
    time.sleep(SLEEP)
    return j["response"]

def upsert(con, table, df, keys):
    if df.empty: return 0
    con.register("df_src", df)
    cols = ", ".join(df.columns)
    pk = ", ".join(keys)
    set_cols = [c for c in df.columns if c not in keys]
    set_clause = ", ".join([f"{c}=excluded.{c}" for c in set_cols])
    con.execute(f"""
        INSERT INTO {table} ({cols})
        SELECT {cols} FROM df_src
        ON CONFLICT ({pk}) DO UPDATE SET {set_clause}
    """)
    con.unregister("df_src")
    return len(df)

def parse_matchday(round_txt: str) -> int | None:
    if not round_txt:
        return None
    parts = round_txt.split("-")
    tail = parts[-1].strip()
    return int(tail) if tail.isdigit() else None

def ingest_dim_league(con):
    resp = api_get("leagues", {})   # can filter by country later
    rows = []
    for d in resp:
        lg = d.get("league",{}) or {}
        ct = d.get("country",{}) or {}
        rows.append({
            "league_id": lg.get("id"),
            "league_name": lg.get("name"),
            "country_name": ct.get("name"),
            "country_code": ct.get("code"),
        })
    df = pd.DataFrame(rows).drop_duplicates("league_id")
    return upsert(con, "dim_league", df, ["league_id"])

def ingest_dim_team(con, league_id:int, season:int):
    teams = api_get("teams", {"league": league_id, "season": season})
    rows=[]
    for t in teams:
        tm = t.get("team",{}) or {}
        rows.append({
            "team_id": tm.get("id"),
            "team_name": tm.get("name"),
            "country_name": (t.get("country") or {}).get("name"),
        })
    df = pd.DataFrame(rows).drop_duplicates("team_id")
    return upsert(con, "dim_team", df, ["team_id"])

def ingest_dim_player(con, league_id:int, season:int):
    page = 1; total=0
    while True:
        resp = api_get("players", {"league": league_id, "season": season, "page": page})
        if not resp: break
        rows=[]
        for rec in resp:
            pl = rec.get("player",{}) or {}
            rows.append({
                "player_id": pl.get("id"),
                "player_name": pl.get("name"),
                "firstname": pl.get("firstname"),
                "lastname": pl.get("lastname"),
                "nationality": pl.get("nationality"),
                "birth_date": (pl.get("birth") or {}).get("date"),
                "height": pl.get("height"),
                "weight": pl.get("weight"),
            })
        df = pd.DataFrame(rows).drop_duplicates("player_id")
        total += upsert(con, "dim_player", df, ["player_id"])
        page += 1
    return total

def ingest_fixtures(con, league_id:int, season:int):
    # Pull season fixtures (API allows filters like from/to, round, date)
    resp = api_get("fixtures", {"league": league_id, "season": season})
    rows=[]
    for fx in resp:
        league = fx.get("league",{}) or {}
        fixture = fx.get("fixture",{}) or {}
        teams = fx.get("teams",{}) or {}
        goals = fx.get("goals",{}) or {}
        rows.append({
            "fixture_id": fixture.get("id"),
            "league_id": league.get("id"),
            "season": league.get("season"),
            "round": league.get("round"),
            "date_utc": pd.to_datetime(fixture.get("date"), utc=True, errors="coerce"),
            "venue_id": (fixture.get("venue") or {}).get("id"),
            "venue_name": (fixture.get("venue") or {}).get("name"),
            "status_short": (fixture.get("status") or {}).get("short"),
            "home_team_id": (teams.get("home") or {}).get("id"),
            "away_team_id": (teams.get("away") or {}).get("id"),
            "home_goals": goals.get("home"),
            "away_goals": goals.get("away"),
            "referee": fixture.get("referee"),
            "updated_ts": pd.Timestamp.utcnow(),
        })
    df = pd.DataFrame(rows)
    return upsert(con, "fact_fixtures", df, ["fixture_id"])

def ingest_player_stats(con, league_id:int, season:int):
    page=1; total=0
    while True:
        resp = api_get("players", {"league": league_id, "season": season, "page": page})
        if not resp: break
        rows=[]
        for rec in resp:
            player = rec.get("player",{}) or {}
            for s in rec.get("statistics") or []:
                team   = s.get("team",{}) or {}
                games  = s.get("games",{}) or {}
                shots  = s.get("shots",{}) or {}
                goals  = s.get("goals",{}) or {}
                passes = s.get("passes",{}) or {}
                tackles= s.get("tackles",{}) or {}
                duels  = s.get("duels",{}) or {}
                drib   = s.get("dribbles",{}) or {}
                fouls  = s.get("fouls",{}) or {}
                cards  = s.get("cards",{}) or {}
                rows.append({
                    "league_id": league_id,
                    "season": season,
                    "fixture_id": None,  # this endpoint is season aggregates per player/team
                    "team_id": team.get("id"),
                    "player_id": player.get("id"),
                    "minutes": games.get("minutes"),
                    "rating": games.get("rating"),
                    "shots_total": shots.get("total"), "shots_on": shots.get("on"),
                    "goals": goals.get("total"), "assists": goals.get("assists"),
                    "passes_total": passes.get("total"), "passes_key": passes.get("key"),
                    "tackles": tackles.get("total"),
                    "interceptions": tackles.get("interceptions"),
                    "duels_total": duels.get("total"), "duels_won": duels.get("won"),
                    "dribbles_attempts": drib.get("attempts"), "dribbles_success": drib.get("success"),
                    "fouls_committed": fouls.get("committed"), "fouls_drawn": fouls.get("drawn"),
                    "yellow": cards.get("yellow"), "red": cards.get("red"),
                    "updated_ts": pd.Timestamp.utcnow(),
                })
        df = pd.DataFrame(rows)
        total += upsert(con, "fact_player_stats", df, ["league_id","season","player_id","team_id","fixture_id"])
        page += 1
    return total

def ingest_injuries(con, league_id:int, season:int):
    resp = api_get("injuries", {"league": league_id, "season": season})
    rows=[]
    for e in resp:
        player = e.get("player",{}) or {}
        team   = e.get("team",{}) or {}
        fixture= e.get("fixture",{}) or {}
        rows.append({
            "league_id": league_id, "season": season,
            "team_id": team.get("id"),
            "player_id": player.get("id"),
            "player_name": player.get("name"),
            "status": e.get("status"),
            "reason": e.get("reason"),
            "start_date": pd.to_datetime(e.get("start"), errors="coerce"),
            "expected_return": pd.to_datetime(e.get("end"), errors="coerce"),
            "fixture_id": fixture.get("id"),
            "updated_ts": pd.Timestamp.utcnow(),
        })
    df = pd.DataFrame(rows)
    return upsert(con, "fact_injuries", df, ["league_id","season","team_id","player_id","updated_ts"])

def ingest_odds_by_season(con, league_id:int, season:int,
                          markets=("h2h","over_under","btts")):
    total = 0
    page = 1
    while True:
        resp = api_get("odds", {"league": league_id, "season": season, "page": page})
        if not resp: break
        rows = []
        for r in resp:
            fid = (r.get("fixture") or {}).get("id")
            for bk in r.get("bookmakers") or []:
                bkid = bk.get("id"); bkname = bk.get("name")
                for m in bk.get("bets") or []:
                    market_key = (m.get("name") or "").lower()
                    if markets and all(k not in market_key for k in markets):
                        continue
                    for v in m.get("values") or []:
                        rows.append({
                            "fixture_id": fid,
                            "league_id": league_id,
                            "season": season,
                            "bookmaker_id": bkid,
                            "bookmaker_name": bkname,
                            "market_key": market_key,
                            "selection": v.get("value"),
                            "value": float(v.get("odd")) if v.get("odd") else None,
                            "last_update": pd.to_datetime(v.get("last_update"), errors="coerce"),
                            "updated_ts": pd.Timestamp.utcnow(),
                        })
        df = pd.DataFrame(rows)
        total += upsert(con, "fact_odds", df,
                        ["fixture_id","bookmaker_id","market_key","selection"])
        page += 1
    return total

def _fixture_ids_for(con, league_id:int, season:int, only_finished=True):
    q = """
      SELECT fixture_id
      FROM fact_fixtures
      WHERE league_id = ? AND season = ?
    """
    if only_finished:
        q += " AND status_short IN ('FT','AET','PEN')"
    return con.execute(q, [league_id, season]).fetchdf()["fixture_id"].tolist()

def ingest_player_stats_per_fixture(con, league_id:int, season:int, fixture_ids=None, only_finished=True):
    """
    Pulls per-fixture player stats (/fixtures/players) and upserts into fact_player_stats_match.
    """
    if fixture_ids is None:
        fixture_ids = _fixture_ids_for(con, league_id, season, only_finished=only_finished)

    total = 0
    for i, fid in enumerate(fixture_ids):
        # One call per fixture
        resp = api_get("fixtures/players", {"fixture": fid})
        rows = []
        for tb in resp:
            team  = tb.get("team", {}) or {}
            t_id  = team.get("id")
            t_nm  = team.get("name")
            for p in tb.get("players") or []:
                pl    = p.get("player", {}) or {}
                stats = (p.get("statistics") or [{}])[0]

                player_id   = pl.get("id")
                player_name = pl.get("name")

                games   = stats.get("games", {}) or {}
                shots   = stats.get("shots", {}) or {}
                goals   = stats.get("goals", {}) or {}
                passes  = stats.get("passes", {}) or {}
                tackles = stats.get("tackles", {}) or {}
                duels   = stats.get("duels", {}) or {}
                drib    = stats.get("dribbles", {}) or {}
                fouls   = stats.get("fouls", {}) or {}
                cards   = stats.get("cards", {}) or {}
                offsides= stats.get("offsides")

                rows.append({
                    "fixture_id": fid,
                    "league_id": league_id,
                    "season": season,
                    "team_id": t_id,
                    "player_id": player_id,

                    "player_name": player_name,
                    "team_name": t_nm,
                    "position": games.get("position"),
                    "number": games.get("number"),
                    "is_captain": bool(games.get("captain")) if games.get("captain") is not None else None,

                    "minutes": games.get("minutes"),
                    "rating": games.get("rating"),

                    "shots_total": shots.get("total"), "shots_on": shots.get("on"),
                    "goals": goals.get("total"), "assists": goals.get("assists"), "saves": goals.get("saves"),

                    "passes_total": passes.get("total"), "passes_key": passes.get("key"),
                    "passes_accuracy": passes.get("accuracy"),

                    "tackles": tackles.get("total"), "interceptions": tackles.get("interceptions"),
                    "blocks": tackles.get("blocks"),

                    "duels_total": duels.get("total"), "duels_won": duels.get("won"),

                    "dribbles_attempts": drib.get("attempts"), "dribbles_success": drib.get("success"),

                    "fouls_committed": fouls.get("committed"), "fouls_drawn": fouls.get("drawn"),

                    "yellow": cards.get("yellow"), "red": cards.get("red"),
                    "offsides": offsides,

                    "updated_ts": pd.Timestamp.utcnow(),
                })

        df = pd.DataFrame(rows)
        total += upsert(con, "fact_player_stats_match",
                        df, ["fixture_id", "player_id", "team_id"])

        if (i + 1) % 50 == 0:
            print(f"[players/match] {i+1}/{len(fixture_ids)} fixtures processed")

    return total

def run_full_ingest(leagues: dict[str,int], seasons: list[int]):
    con = duckdb.connect(DB_PATH)
    ensure_schema(con)
    ingest_dim_league(con)

    for country, league_id in leagues.items():
        for season in seasons:
            print(f"== {country} {season} ==")
            # # dimensions
            # ingest_dim_team(con, league_id, season)
            # ingest_dim_player(con, league_id, season)
            # # facts
            # ingest_fixtures(con, league_id, season)
            # # ingest_player_stats(con, league_id, season)
            # ingest_fixtures(con, league_id, season)  # make sure fixtures exist first
            # ingest_player_stats_per_fixture(con, league_id, season, only_finished=True)
            # ingest_standings_snapshot(con, league_id, season)
            try:
                ingest_injuries(con, league_id, season)
            except RuntimeError as e:
                print(f"Injuries skip: {e}")  # some seasons may not have data

            # odds for this league/season (example: only fixtures in this season)
            fids = con.execute("""
                SELECT fixture_id FROM fact_fixtures
                WHERE league_id=? AND season=? AND date_utc IS NOT NULL
            """, [league_id, season]).fetchdf()["fixture_id"].tolist()
            ingest_standings_by_round(con, league_id, season)
            # ingest_odds_by_season(con, league_id, season) 

    con.close()

if __name__ == "__main__":
    leagues = {
        "England": 39,     # Premier League
        "France": 61,      # Ligue 1
        "Germany": 78,     # Bundesliga
        "Italy": 135,      # Serie A
        "Spain": 140,      # La Liga
    }
    seasons = list(range(2023, 2026))  # [2020, 2021, 2022, 2023, 2024, 2025]

    run_full_ingest(leagues, seasons)
