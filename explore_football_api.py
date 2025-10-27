# From API-football documentation:
# You have to replace {endpoint} by the real name of the endpoint you want to call, like leagues or fixtures for example. In all the sample scripts we will use the leagues endpoint as example.
from dotenv import load_dotenv
load_dotenv()
import os, requests, pandas as pd
API_KEY = os.getenv("API_FOOTBALL_KEY")
BASE = "https://v3.football.api-sports.io"
HEADERS = {"x-apisports-key": API_KEY}

# Call the API and get structured JSON
resp = requests.get(f"{BASE}/leagues", headers=HEADERS, timeout=30)
# resp = requests.get(f"{BASE}/odds", headers=HEADERS, timeout=30, params={'league':78, 'season':2025})
resp.raise_for_status()
j = resp.json()  # dict with keys: get, parameters, errors, results, response(list)
# Top-level “response” is a list of league entries
data = j.get("response", [])

# Flatten the common fields into a tidy DataFrame
df_all = pd.json_normalize(data, sep="_")
df_leagues = df_all[["league_id","league_name","league_type","country_name","country_code"]].drop_duplicates()

# explode data into a separate tidy table
df = pd.DataFrame({"seasons": [d.get("seasons", []) for d in data],
                   "league_id": [d.get("league",{}).get("id") for d in data]})
df = df.explode("seasons", ignore_index=True)

df_seasons = pd.concat([
    pd.json_normalize(df["seasons"].dropna(), sep="_").reset_index(drop=True),
    df[["league_id"]].reset_index(drop=True)
], axis=1)

df_seasons = df_seasons[[
    "league_id", "year", "current",
    "coverage_fixtures_events", "coverage_fixtures_lineups",
    "coverage_standings", "coverage_players", "coverage_top_scorers"
]].rename(columns={"coverage_top_scorers": "coverage_topscorers"})

print("Leagues:\n", df_leagues.head(5))
print("\nSeasons:\n", df_seasons.query("year >= 2020 and year <= 2025").head(10))