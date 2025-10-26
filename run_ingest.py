if __name__ == "__main__":
    from src.ingest.loaders import ingest_matches_openligadb, ingest_odds_snapshot

    print("→ Ingesting Bundesliga fixtures via OpenLigaDB…")
    res1 = ingest_matches_openligadb(league="bl1", season="2025")
    print("   inserted:", res1)

    # print("→ Ingesting odds snapshot (H2H, The Odds API)…")
    # res2 = ingest_odds_snapshot()
    # print("   inserted:", res2)

    print("Done. You can query the DuckDB at DUCKDB_PATH.")
