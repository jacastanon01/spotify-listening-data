CREATE TABLE IF NOT EXISTS playback (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    played_at TEXT NOT NULL,
    ms_played INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_playback_played_at ON playback(played_at);