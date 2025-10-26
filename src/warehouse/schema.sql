
-- matches: one row per match (fixture)
CREATE TABLE IF NOT EXISTS matches (
  match_id        BIGINT PRIMARY KEY,
  league_id       INT,
  season          INT,
  kickoff_ts      TIMESTAMP,
  home_id         INT,
  away_id         INT,
  venue           VARCHAR,
  referee         VARCHAR,
  status          VARCHAR,
  inserted_at     TIMESTAMP DEFAULT now()
);

-- lineups: one row per (match, team, player)
CREATE TABLE IF NOT EXISTS lineups (
  match_id         BIGINT,
  team_id          INT,
  player_id        INT,
  is_starter       BOOLEAN,
  position         VARCHAR,
  minutes_expected INT,
  PRIMARY KEY (match_id, team_id, player_id)
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
