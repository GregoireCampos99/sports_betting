# Goal: 
I want to create a soccer-betting process. the idea is to build a robust pipeline of both soccer-related and quotes-related data. I decided to start with the bundesliga to leverage OpenLigaDB for a first approach (free DB). Also, it is less crowded than EPL, which might mean more upside potential. 
Thus:
- football-related data (results, standings, lineup...): OpenLigaDB
- quotes: The Odds API (first approach: we want to keep it free. we will only grab quotes 24h before kick off. ~306 games per season, The Odds limit is 500 requests / month)
    can also use Sportsbook API (50/day), OddsAPI.io (500/month) or API-Football (100/day)

We need to get injured players, player stats, team stats (can be inferred from player stats if no transfer during the season). After looking around, I decided to go with the $19 monthly subscription with api-football, to get good data coverage - the end goal being to train a model, I need a solid dataset.
In order to get more familiar with the datastructure, I built a script explore_data_pipeline.py, that realizes calls to build in-house SQL-like datasets.

# set up the environment:

in powershell (dl python 3.11.9 if needed):
    py -3.11 -m venv .venv

then:
    Set-ExecutionPolicy -Scope Process -ExecutionPolicy Bypass
    .\.venv\Scripts\Activate.ps1

then check:
    python -c "import ssl; print(ssl.OPENSSL_VERSION)"

it should return something like: "OpenSSL 3.0.13 30 Jan 2024"


you might need to install the dependencies
    pip install httpx pandas duckdb python-dotenv pyarrow

finally run 
    python run_ingest.py

# Data
I use duckdb. I can't export it in here - not sure why, still investigating. run_ingest.py does create the database.
You will need an API key for: 
- TheOdds (free, limited feed)


# Done

# In Progress: gathering and processing data: 
- improve coverage for warehouse.main.fact_standings_snapshot (currently one per season and team)
- improve coverage for odds (looks like it is empty on API-football - might need to revert back to TheOdds)

# To do 
- betting engine