CREATE TABLE IF NOT EXISTS episode (
    id TEXT PRIMARY KEY UNIQUE,
    uri TEXT UNIQUE NOT NULL,
    episode_name TEXT,
    show_name TEXT
);