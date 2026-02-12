
PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS player (
  id INTEGER PRIMARY KEY,
  name TEXT NOT NULL UNIQUE
);

CREATE TABLE IF NOT EXISTS event (
  id INTEGER PRIMARY KEY,
  name TEXT NOT NULL,
  -- NOTE: avoid hard CHECK constraints on mode so the schema can evolve
  -- without table rebuilds/renames.
  mode TEXT NOT NULL,
  created_at TEXT NOT NULL,
  notes TEXT DEFAULT '',
  -- For group tournaments: format of the *group phase* (A/B round-robin)
  group_best_of INTEGER DEFAULT 1,
  playoff_best_of INTEGER DEFAULT 1,
  status TEXT NOT NULL DEFAULT 'draft'
);

CREATE TABLE IF NOT EXISTS event_player (
  event_id INTEGER NOT NULL REFERENCES event(id) ON DELETE CASCADE,
  player_id INTEGER NOT NULL REFERENCES player(id) ON DELETE RESTRICT,
  PRIMARY KEY (event_id, player_id)
);

CREATE TABLE IF NOT EXISTS match (
  id INTEGER PRIMARY KEY,
  event_id INTEGER NOT NULL REFERENCES event(id) ON DELETE CASCADE,
  kind TEXT NOT NULL CHECK (kind IN ('duel','multiplayer')),
  stage TEXT NOT NULL CHECK (stage IN ('main','semi','final')),
  table_no INTEGER DEFAULT NULL,
  best_of INTEGER DEFAULT NULL,
  player_a INTEGER DEFAULT NULL REFERENCES player(id) ON DELETE RESTRICT,
  player_b INTEGER DEFAULT NULL REFERENCES player(id) ON DELETE RESTRICT,
  round_index INTEGER DEFAULT NULL,
  created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS game (
  id INTEGER PRIMARY KEY,
  match_id INTEGER NOT NULL REFERENCES match(id) ON DELETE CASCADE,
  game_no INTEGER NOT NULL,
  winner_player_id INTEGER NOT NULL REFERENCES player(id) ON DELETE RESTRICT,
  loser_player_id INTEGER NOT NULL REFERENCES player(id) ON DELETE RESTRICT,
  delta_life INTEGER DEFAULT NULL,
  UNIQUE (match_id, game_no)
);

CREATE TABLE IF NOT EXISTS multiplayer_rank (
  match_id INTEGER NOT NULL REFERENCES match(id) ON DELETE CASCADE,
  player_id INTEGER NOT NULL REFERENCES player(id) ON DELETE RESTRICT,
  rank INTEGER NOT NULL,
  PRIMARY KEY (match_id, player_id),
  UNIQUE (match_id, rank)
);

CREATE TABLE IF NOT EXISTS audit_log (
  id INTEGER PRIMARY KEY,
  event_id INTEGER REFERENCES event(id) ON DELETE CASCADE,
  created_at TEXT NOT NULL,
  kind TEXT NOT NULL,
  payload_json TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_match_event ON match(event_id);
CREATE INDEX IF NOT EXISTS idx_game_match ON game(match_id);
CREATE INDEX IF NOT EXISTS idx_mrank_match ON multiplayer_rank(match_id);
