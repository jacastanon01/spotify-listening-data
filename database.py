import sqlite3
from typing import Tuple

from normalize import ListeningHistoryEntry, extract_id_from_uri


def initilize_tables(conn: sqlite3.Connection) -> None:
    # conn = sqlite3.connect(":memory:///data.db")
    cursor = conn.cursor()

    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS track (
            id TEXT PRIMARY KEY UNIQUE,
            uri TEXT UNIQUE NOT NULL,
            track_name TEXT,
            artist_name TEXT
        );
    """
    )

    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS episode (
            id TEXT PRIMARY KEY UNIQUE,
            uri TEXT UNIQUE NOT NULL,
            episode_name TEXT,
            show_name TEXT
        ); 
    """
    )

    cursor.execute(
        """ 
        CREATE TABLE IF NOT EXISTS playback (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            played_at TEXT NOT NULL,
            ms_played INTEGER NOT NULL
        );
    """
    )

    cursor.execute(
        """ 
        CREATE TABLE IF NOT EXISTS playback_activity (
            playback_id INTEGER,
            track_id TEXT,
            episode_id TEXT,
            FOREIGN KEY (playback_id) REFERENCES playback (id) ON DELETE CASCADE,
            FOREIGN KEY (track_id) REFERENCES track (id) ON DELETE CASCADE,
            FOREIGN KEY (episode_id) REFERENCES episode (id) ON DELETE CASCADE
        )
        """
    )


def insert_into_db(conn: sqlite3.Connection, entry: ListeningHistoryEntry) -> None:
    cursor = conn.cursor()

    try:
        track_id = extract_id_from_uri(entry.get("track_uri"))
        episode_id = extract_id_from_uri(entry.get("episode_uri"))

        if track_id is not None:
            insert_track_or_episode(
                cursor=cursor,
                entity_type="track",
                entry=(
                    track_id,
                    entry.get("track_uri"),
                    entry.get("track_name"),
                    entry.get("artist_name"),
                ),
            )

        elif episode_id is not None:
            insert_track_or_episode(
                cursor=cursor,
                entity_type="episode",
                entry=(
                    episode_id,
                    entry.get("episode_uri"),
                    entry.get("episode_name"),
                    entry.get("show_name"),
                ),
            )

        cursor.execute(
            """
            INSERT INTO playback (played_at, ms_played)
            VALUES (?, ?);
        """,
            (entry.get("played_at"), entry.get("ms_played")),
        )

        playback_id = cursor.lastrowid

        cursor.execute(
            """
            INSERT INTO playback_activity (playback_id, track_id, episode_id)
            VALUES (?, ?, ?)
        """,
            (playback_id, track_id, episode_id),
        )

    except Exception as e:
        print(f"Error: {e}")
        conn.rollback()

    finally:
        cursor.close()


def insert_track_or_episode(
    cursor: sqlite3.Cursor, entity_type: str, entry: Tuple[str, str, str, str]
) -> None:
    if entity_type == "track":
        cursor.execute(
            """
            INSERT OR IGNORE INTO track (id, uri, track_name, artist_name) 
            VALUES (?, ?, ?, ?); 
        """,
            entry,
        )

    elif entity_type == "episode":
        cursor.execute(
            """
            INSERT OR IGNORE INTO episode (id, uri, episode_name, show_name)
            VALUES (?, ?, ?, ?);
        """,
            entry,
        )

    else:
        raise ValueError(f"Invalid entity type: {entity_type}")
