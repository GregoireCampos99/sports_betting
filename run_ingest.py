if __name__ == "__main__":
    # we switched from OpenDB to API-football
    from src.ingest.loaders import ingest_matches_openligadb, ingest_odds_snapshot 
    from src.ingest.explore_data_pipeline import run_full_ingest

    print("→ Getting football data…")
    leagues = {
        "England": 39,     # Premier League
        "France": 61,      # Ligue 1
        "Germany": 78,     # Bundesliga
        "Italy": 135,      # Serie A
        "Spain": 140,      # La Liga
    }
    seasons = list(range(2020, 2023))  # We ran [2020, 2021, 2022, 2023, 2024, 2025] for EPL, otherwise [2023, 2024, 2025]
    run_full_ingest(leagues, seasons)


    print("→ Ingesting odds snapshot (H2H, The Odds API)…") # For now, we can only get 2025 quotes. We got it for all 5 leagues. Would have to get a plan to have more history.
    dic_sport_key = {
        'EPL': 'soccer_epl',
        'Ligue1': 'soccer_france_ligue_one',
        'LaLiga': 'soccer_spain_la_liga',
        'SerieA': 'soccer_italy_serie_a',
        'Bundesliga': 'soccer_germany_bundesliga'
    }
    for league in dic_sport_key.keys():
        print(f"Gathering quotes for {league}")
        res2 = ingest_odds_snapshot(sport_key=dic_sport_key[league])
        print("   inserted:", res2)

    print("Done. You can query the DuckDB at DUCKDB_PATH.")
