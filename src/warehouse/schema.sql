-- Dimensions
CREATE TABLE IF NOT EXISTS dim_league (
  league_id INTEGER PRIMARY KEY,
  league_name VARCHAR,
  country_name VARCHAR,
  country_code VARCHAR
);

CREATE TABLE IF NOT EXISTS dim_team (
  team_id INTEGER PRIMARY KEY,
  team_name VARCHAR,
  country_name VARCHAR
);

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

-- Core facts
CREATE TABLE IF NOT EXISTS fact_fixtures (
  fixture_id INTEGER PRIMARY KEY,
  league_id INTEGER,
  season INTEGER,
  round VARCHAR,
  date_utc TIMESTAMP,
  venue_id INTEGER,
  venue_name VARCHAR,
  status_short VARCHAR,   -- 'NS','1H','2H','HT','FT',...
  home_team_id INTEGER,
  away_team_id INTEGER,
  home_goals INTEGER,
  away_goals INTEGER,
  referee VARCHAR,
  updated_ts TIMESTAMP
);

CREATE TABLE IF NOT EXISTS fact_player_stats (
  league_id INTEGER,
  season INTEGER,
  fixture_id INTEGER,
  team_id INTEGER,
  player_id INTEGER,
  minutes INTEGER,
  rating VARCHAR,
  shots_total INTEGER, shots_on INTEGER,
  goals INTEGER, assists INTEGER,
  passes_total INTEGER, passes_key INTEGER,
  tackles INTEGER, interceptions INTEGER,
  duels_total INTEGER, duels_won INTEGER,
  dribbles_attempts INTEGER, dribbles_success INTEGER,
  fouls_committed INTEGER, fouls_drawn INTEGER,
  yellow INTEGER, red INTEGER,
  updated_ts TIMESTAMP,
  PRIMARY KEY (league_id, season, fixture_id, player_id, team_id)
);

-- Season aggregates per player
CREATE TABLE IF NOT EXISTS fact_player_stats_season (
  league_id INTEGER,
  season INTEGER,
  team_id INTEGER,
  player_id INTEGER,
  minutes INTEGER,
  rating VARCHAR,
  shots_total INTEGER, shots_on INTEGER,
  goals INTEGER, assists INTEGER,
  passes_total INTEGER, passes_key INTEGER,
  tackles INTEGER, interceptions INTEGER,
  duels_total INTEGER, duels_won INTEGER,
  dribbles_attempts INTEGER, dribbles_success INTEGER,
  fouls_committed INTEGER, fouls_drawn INTEGER,
  yellow INTEGER, red INTEGER,
  updated_ts TIMESTAMP,
  PRIMARY KEY (league_id, season, player_id, team_id)
);

CREATE TABLE IF NOT EXISTS fact_player_stats_match (
  fixture_id INTEGER,
  league_id INTEGER,
  season INTEGER,
  team_id INTEGER,
  player_id INTEGER,
  player_name VARCHAR,
  team_name   VARCHAR,
  position VARCHAR,
  number INTEGER,
  is_captain BOOLEAN,
  minutes INTEGER,
  rating VARCHAR,
  shots_total INTEGER, shots_on INTEGER,
  goals INTEGER, assists INTEGER, saves INTEGER,
  passes_total INTEGER, passes_key INTEGER, passes_accuracy INTEGER,
  tackles INTEGER, interceptions INTEGER, blocks INTEGER,
  duels_total INTEGER, duels_won INTEGER,
  dribbles_attempts INTEGER, dribbles_success INTEGER,
  fouls_committed INTEGER, fouls_drawn INTEGER,
  yellow INTEGER, red INTEGER,
  offsides INTEGER,
  updated_ts TIMESTAMP,
  PRIMARY KEY (fixture_id, player_id, team_id)
);



CREATE TABLE IF NOT EXISTS fact_standings_snapshot (
  league_id INTEGER,
  season INTEGER,
  matchday INTEGER,      -- or date snapshot
  team_id INTEGER,
  rank INTEGER,
  points INTEGER,
  played INTEGER, wins INTEGER, draws INTEGER, losses INTEGER,
  gf INTEGER, ga INTEGER, gd INTEGER,
  form VARCHAR,
  snapshot_ts TIMESTAMP,       -- when you pulled it
  PRIMARY KEY (league_id, season, matchday, team_id)
);

DROP TABLE IF EXISTS fact_injuries
CREATE TABLE IF NOT EXISTS fact_injuries (
  league_id INTEGER,
  season INTEGER,
  team_id INTEGER,
  player_id INTEGER,
  player_name VARCHAR,
  status VARCHAR,               -- 'Injured', 'Doubtful', etc.
  reason VARCHAR,
  start_date date,
  expected_return date,
  fixture_id INTEGER,
  updated_ts TIMESTAMP,
  PRIMARY KEY (league_id, season, team_id, player_id, start_date)
);

-- Odds (one row per fixture-bookmaker-market selection; granular)
CREATE TABLE IF NOT EXISTS fact_odds (
  fixture_id INTEGER,
  league_id INTEGER,
  season INTEGER,
  bookmaker_id INTEGER,
  bookmaker_name VARCHAR,
  market_key VARCHAR,         -- e.g. 'h2h','over_under','both_teams_to_score'
  selection VARCHAR,          -- 'Home','Draw','Away' or 'Over 2.5',...
  value DOUBLE,
  last_update TIMESTAMP,
  updated_ts TIMESTAMP,
  PRIMARY KEY (fixture_id, bookmaker_id, market_key, selection)
);


-- odds snapshots: pre-match odds with timestamps from multiple books
CREATE TABLE IF NOT EXISTS odds (
  match_id   BIGINT,
  book       VARCHAR,
  market     VARCHAR,      -- e.g., 'h2h'
  outcome    VARCHAR,      -- 'home'|'draw'|'away'
  price_dec  DOUBLE,
  ts         TIMESTAMP,
  is_closing BOOLEAN DEFAULT FALSE,
  PRIMARY KEY (match_id, book, market, outcome, ts)
);
