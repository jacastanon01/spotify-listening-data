import os
import sqlite3
from typing import Optional, Tuple


from config import SQL_QUERY_PATHS
from normalize import IListeningHistoryEntry, extract_id_from_uri

DATABASE_PATH = "audio.db"


def execute_sql_from_file(cursor: sqlite3.Cursor, file: str) -> None:
    try:
        with open(file, "r") as f:
            sql_statements = f.read().split(";")

            for statement in sql_statements:
                if statement.split():
                    cursor.execute(statement)

            print(cursor.fetchone())
    except IOError as e:
        print(e)


def initialize_db(conn: sqlite3.Connection) -> None:
    # conn = sqlite3.connect(":memory:///data.db")
    cursor = conn.cursor()

    for path in SQL_QUERY_PATHS:
        execute_sql_from_file(cursor=cursor, file=path)

    conn.commit()


def reset_and_connect_db() -> sqlite3.Connection:
    """Delete the existing database file and create a new one."""
    if os.path.exists(DATABASE_PATH):
        os.remove(DATABASE_PATH)

    conn = sqlite3.connect("audio.db")

    initialize_db(conn)
    return conn


def insert_into_db(conn: sqlite3.Connection, entry: IListeningHistoryEntry) -> None:
    cursor = conn.cursor()

    try:
        track_uri = entry.get("track_uri")
        episode_uri = entry.get("episode_uri")

        track_id: Optional[str] = None
        episode_id: Optional[str] = None

        if track_uri:
            track_id = extract_id_from_uri(track_uri)
            track_name = entry.get("track_name")
            artist_name = entry.get("artist_name")

            insert_track_or_episode(
                cursor=cursor,
                entity_type="track",
                entry=(track_id, track_uri, track_name, artist_name),
            )

        if episode_uri:
            episode_id = extract_id_from_uri(episode_uri)
            episode_name = entry.get("episode_name")
            show_name = entry.get("show_name")

            insert_track_or_episode(
                cursor=cursor,
                entity_type="episode",
                entry=(
                    episode_id,
                    episode_uri,
                    episode_name,
                    show_name,
                ),
            )

        if track_id or episode_id:
            playback_id = insert_playback(cursor, entry)
            if playback_id is not None:
                if track_id:
                    insert_track_activity(cursor, playback_id, track_id)
                if episode_id:
                    insert_episode_activity(cursor, playback_id, episode_id)
            else:
                print("Failed to insert playback record")

        conn.commit()

    except Exception as e:
        print(f"Error: {e}")
        conn.rollback()

    finally:
        cursor.close()


def insert_playback(
    cursor: sqlite3.Cursor, entry: IListeningHistoryEntry
) -> int | None:
    cursor.execute(
        """
        INSERT INTO playback (played_at, ms_played)
        VALUES (?, ?);
    """,
        (entry.get("played_at"), entry.get("ms_played")),
    )
    return cursor.lastrowid


def insert_track_activity(
    cursor: sqlite3.Cursor, playback_id: int, track_id: str
) -> None:
    cursor.execute(
        """
        INSERT INTO track_activity (playback_id, track_id)
        VALUES (?, ?)
    """,
        (playback_id, track_id),
    )


def insert_episode_activity(
    cursor: sqlite3.Cursor, playback_id: int, episode_id: str
) -> None:
    cursor.execute(
        """
        INSERT INTO episode_activity (playback_id, episode_id)
        VALUES (?, ?)
    """,
        (playback_id, episode_id),
    )


# def insert_playback_activity(
#     cursor: sqlite3.Cursor,
#     entry: IListeningHistoryEntry,
#     track_id: Optional[str] = None,
#     episode_id: Optional[str] = None,
# ):
#     if track_id is None and episode_id is None:
#         return

#     cursor.execute(
#         """
#         INSERT INTO playback (played_at, ms_played)
#         VALUES (?, ?);
#     """,
#         (entry.get("played_at"), entry.get("ms_played")),
#     )

#     playback_id = cursor.lastrowid

#     if track_id:
#         cursor.execute(
#             """
#             INSERT INTO track_activity (playback_id, track_id)
#             VALUES (?, ?)
#             """,
#             (playback_id, track_id),
#         )
#     if episode_id:
#         cursor.execute(
#             """
#             INSERT INTO episode_activity (playback_id, episode_id)
#             VALUES (?, ?)
#              """,
#             (playback_id, episode_id),
#         )


def insert_track_or_episode(
    cursor: sqlite3.Cursor,
    entity_type: str,
    entry: Tuple[str, str, str | None, str | None],
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
