# Warehouse schema

# === table dim_league ===
| Column | Type | Description |
|---------|------|-------------|
| league_id | int | league identifier |
| league_name | str | league name |
| country_name | str | country name |
| country_code | str | country code |

# === table dim_team ===
| Column | Type | Description |
|---------|------|-------------|
| team_id | int | team identifier |
| team_name | str | team name |
| country_name | str | country name |

# === table fact_fixtures ===
| Column | Type | Description |
|---------|------|-------------|
| fixture_id | int | fixture identifier |
| league_id | int |league identifier |
| season | int | season (start year) |
| round | | str | | round in the championship (i.e. day 1 of the championship to day 34 when 18 teams) |
| date_utc | timestamp | date of the game
| venue_id | int | identifier of the stadium
| venue_name | str | name of the stadium
| status_short | str | 
| home_team_id | int | home team identifier |
| away_team_id | int | away team identifier |
| home_goals | int | number of goals home team |
| away_goals | int | number of goals away team |
| referee | str | referee name |
| updated_ts | timestamp | timestamp of the update |

# === table fact_player_stats_match - player stats before each Fixture ===
| Column | Type | Description |
|---------|------|-------------|
| fixture_id | int | fixture identifier |
| league_id | int |league identifier |
| season | int | season (start year) |
| team_id | int | team identifier |
| team_name | str | team name |
| player_id | int | player identifier |
| player_name | str | player name |
| position | str | position of the player |
| number | int | number of the player's jersey |
| is_captain | bool | is the player also the captain |
| minutes | int | minutes played by the player during the game |
| rating | float | rating of the player during the game |
| shots_total | int | shots made by the player |
| shots_on | int | shots on target made by the player |
| goals | int | goals scored by the player |
| assists | int | assists made by the player |
| saves | int | saves made by the player |
| passes_total | int | number of passes made by the player |
| passes_key | int | number of key passes made by the player |
| passes_accuracy | int | accuracy of the passes made by the player |
| tackles | int | tackles made by the player |
| interceptions | int | interceptions made by the player |
| blocks | int | blobks made by the player |
| duels_total | int | duels played by the player |
| duels_won | int | duels won by the player |
| dribbles_attempts | int | dribbles attempted by the player |
| dribbles_success | int | successful dribbles made by the player |
| fouls_committed | int | fouls commited by the player |
| fouls_drawn | int | fouls drawn by the player |
| yellow | int | yellow card received by the player |
| red | int | red card received by the player |
| offsides | int | offsides made by the player |
| updated_ts | timestamp | timestamp of the update |
| PRIMARY KEY (fixture_id, player_id, team_id) | str | primary key of the table |

# === table: fact_injuries ===
| Column | Type | Description |
|---------|------|-------------|
| league_id | int | league identifier |
| season | int | season |
| team_id | int | team identifier |
| player_id | int | player identifier |
| player_name | str | player name |
| status | str | player's status |
| reason | str | player injury |
| start_date | date | player injury's start date (usually not in the db) |
| expected_return | date | player's expected return (usually not in the db) |
| fixture_id | int | fixture identifier |
| updated_ts | timestamp | timestamp of the update |
| PRIMARY KEY (league_id, season, team_id, player_id, updated_ts) | str | primary key of the table |

# === Fact: Odds ===
| Column | Type | Description |
|---------|------|-------------|
| fixture_id | int | fixture identifier |
| league_id | int | league identifier |
| season | int | season |
| bookmaker_id | int | bookmaker identifier |
| bookmaker_name | str | bookmaker name |
| market_key | str | market identifier |
| selection | str | 
| value | float | value of the quote |
| last_update | timestamp | last update of the quote |
| updated_ts | timestamp | timestamp of the update |
| PRIMARY KEY (fixture_id, bookmaker_id, market_key, selection) | str | primary key of the table |

