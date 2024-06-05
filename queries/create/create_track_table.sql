CREATE TABLE IF NOT EXISTS track (
    id TEXT PRIMARY KEY UNIQUE,
    uri TEXT UNIQUE NOT NULL,
    track_name TEXT,
    artist_name TEXT,
    duration_ms INTEGER
)