CREATE TABLE IF NOT EXISTS episode_activity (
    playback_id INTEGER,
    episode_id TEXT,
    FOREIGN KEY (playback_id) REFERENCES playback (id) ON DELETE CASCADE,
    FOREIGN KEY (episode_id) REFERENCES episode (id) ON DELETE CASCADE,
    PRIMARY KEY (playback_id, episode_id)
);