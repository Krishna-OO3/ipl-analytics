CREATE TABLE IF NOT EXISTS teams (
    team_id   SERIAL PRIMARY KEY,
    team_name VARCHAR(100) UNIQUE NOT NULL
);

CREATE TABLE IF NOT EXISTS venues (
    venue_id   SERIAL PRIMARY KEY,
    venue_name VARCHAR(200),
    city       VARCHAR(100)
);

CREATE TABLE IF NOT EXISTS players (
    player_id    SERIAL PRIMARY KEY,
    player_name  VARCHAR(100) NOT NULL,
    cricsheet_id VARCHAR(20) UNIQUE
);

CREATE TABLE IF NOT EXISTS matches (
    match_id        VARCHAR(20) PRIMARY KEY,
    season          VARCHAR(10),
    match_date      DATE,
    venue_id        INT REFERENCES venues(venue_id),
    team1_id        INT REFERENCES teams(team_id),
    team2_id        INT REFERENCES teams(team_id),
    toss_winner_id  INT REFERENCES teams(team_id),
    toss_decision   VARCHAR(10),
    winner_id       INT REFERENCES teams(team_id),
    win_by_runs     INT,
    win_by_wickets  INT,
    player_of_match INT REFERENCES players(player_id),
    match_number    INT
);

CREATE TABLE IF NOT EXISTS deliveries (
    delivery_id         SERIAL PRIMARY KEY,
    match_id            VARCHAR(20) REFERENCES matches(match_id),
    inning              INT,
    over_num            INT,
    ball_num            INT,
    batter_id           INT REFERENCES players(player_id),
    bowler_id           INT REFERENCES players(player_id),
    non_striker_id      INT REFERENCES players(player_id),
    runs_batter         INT DEFAULT 0,
    runs_extras         INT DEFAULT 0,
    runs_total          INT DEFAULT 0,
    extra_type          VARCHAR(20),
    is_wicket           BOOLEAN DEFAULT FALSE,
    wicket_kind         VARCHAR(30),
    player_dismissed_id INT REFERENCES players(player_id)
);
