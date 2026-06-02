CREATE TABLE IF NOT EXISTS leaderboard_metrics (
  time          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  contestant_id TEXT NOT NULL,
  p50_us        BIGINT,
  p90_us        BIGINT,
  p99_us        BIGINT,
  tps           DOUBLE PRECISION,
  correctness   DOUBLE PRECISION,
  total_orders  BIGINT DEFAULT 0,
  correct_fills BIGINT DEFAULT 0,
  composite     DOUBLE PRECISION,
  bot_count     INT,
  PRIMARY KEY (contestant_id)
);

CREATE INDEX IF NOT EXISTS idx_leaderboard_time ON leaderboard_metrics (time DESC);

CREATE TABLE IF NOT EXISTS test_runs (
  run_id        UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  contestant_id TEXT NOT NULL,
  binary_sha256 TEXT NOT NULL,
  bot_config    JSONB NOT NULL,
  started_at    TIMESTAMPTZ NOT NULL,
  ended_at      TIMESTAMPTZ,
  outcome       TEXT
);

CREATE INDEX IF NOT EXISTS idx_test_runs_contestant ON test_runs (contestant_id);
