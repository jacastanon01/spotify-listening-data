CREATE TABLE IF NOT EXISTS track_activity (
    playback_id INTEGER,
    track_id TEXT,
    FOREIGN KEY (playback_id) REFERENCES playback (id) ON DELETE CASCADE,
    FOREIGN KEY (track_id) REFERENCES track (id) ON DELETE CASCADE,
    PRIMARY KEY (playback_id, track_id)
)