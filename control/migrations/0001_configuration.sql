CREATE TABLE IF NOT EXISTS runner_config (
  bucket TEXT NOT NULL,
  id TEXT NOT NULL,
  payload TEXT NOT NULL,
  updated INTEGER NOT NULL,
  PRIMARY KEY (bucket, id)
);
