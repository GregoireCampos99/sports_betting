We will build the full pipeline for EPL, establish your baseline; then test the model on “less efficient” league(s) for edge.

Go with bundesliga to leverage OpenLigaDB for a first approach (free DB). Thus:
- football-related data (results, standings, lineup...): OpenLigaDB
- quotes: The Odds API (first approach: we want to keep it free. we will only grab quotes 24h before kick off. ~306 games per season, The Odds limit is 500 requests / month)
    can also use Sportsbook API (50/day), OddsAPI.io (500/month) or API-Football (100/day)

What to build first (milestones)

M0 – Data skeleton

Set up DuckDB/Postgres; write ingest/api_football.py to pull fixtures+lineups and odds_theoddsapi.py to pull odds snapshots; store to matches and odds. 
api-sports +2
api-football +2

M1 – Feature table @ T-minus

Add Elo (or import ClubElo) + rest/congestion + odds dispersion features; materialize to features_match_tminus_2h. 
Club Elo

M2 – Baseline model

Multinomial logistic or LightGBM → calibrated probs; evaluate logloss, Brier, profit (flat stakes), and CLV on a 2-season walk-forward.

M3 – Backtester

Implement bankroll simulation with fractional Kelly, edge threshold, line shopping, and limits; output ROI, Sharpe, max DD, CLV.

M4 – Upgrade data

Add StatsBomb Open Data for leagues it covers; engineer style/matchup features; compare against market-only model. 
GitHub
+1

M5 – Exchange & closing line

Pull Betfair historical prices for closing lines; re-score CLV and slippage realism. 
Betfair Developer Center




Repo layout 
soccer-betting/
├── README.md
├── pyproject.toml
├── data/                      # gitignored (parquet/csv)
├── conf/                      # API keys, dbt profiles (local .env)
├── src/
│   ├── ingest/
│   │   ├── api_football.py
│   │   ├── odds_theoddsapi.py
│   │   ├── betfair_hist.py
│   │   └── loaders.py
│   ├── warehouse/
│   │   ├── schema.sql
│   │   └── io.py               # DuckDB/Postgres helpers
│   ├── features/
│   │   ├── build_match_features.py
│   │   └── elo.py
│   ├── models/
│   │   ├── train_multinomial.py
│   │   ├── train_poisson.py
│   │   ├── calibrate.py        # isotonic/platt
│   │   └── infer.py
│   ├── backtest/
│   │   ├── simulate.py
│   │   └── metrics.py          # ROI, CLV, Sharpe, DD
│   └── utils/
│       └── odds.py
├── notebooks/
│   ├── 01_eda.ipynb
│   └── 02_feature_checks.ipynb
└── tests/

Core formulas & tiny snippets

Implied probability (decimal odds) and edge:

def implied_prob(decimal_odds: float) -> float:
    return 1.0 / decimal_odds

def edge(model_prob: float, book_odds: float) -> float:
    return model_prob - implied_prob(book_odds)  # >0 means +EV


Fractional Kelly bet size (for a single outcome):

def kelly_fraction(p: float, dec_odds: float, frac: float = 0.5, cap: float = 0.02):
    b = dec_odds - 1.0
    f_star = (p*(b+1) - 1) / b     # full Kelly on decimal odds
    stake = max(0.0, f_star) * frac
    return min(stake, cap)          # cap at, say, 2% bankroll


EV and CLV tracking:

def expected_value(p: float, dec_odds: float) -> float:
    return p*dec_odds - 1.0

# CLV: compare your bet's odds vs. closing odds
def clv_pct(my_dec: float, closing_dec: float) -> float:
    return (my_dec - closing_dec) / closing_dec


Time-based CV (no leakage):

Train on seasons/weeks ≤ T, validate on (T, T+Δ], rebuild features only with information available at or before the bet timestamp.

