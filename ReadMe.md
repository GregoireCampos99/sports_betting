We will build the full pipeline for EPL, establish your baseline; then test the model on “less efficient” league(s) for edge.

Go with bundesliga to leverage OpenLigaDB for a first approach (free DB). Thus:
- football-related data (results, standings, lineup...): OpenLigaDB
- quotes: The Odds API (first approach: we want to keep it free. we will only grab quotes 24h before kick off. ~306 games per season, The Odds limit is 500 requests / month)
    can also use Sportsbook API (50/day), OddsAPI.io (500/month) or API-Football (100/day)