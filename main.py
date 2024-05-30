import os
import json
import sqlite3
from typing import Dict, List, Tuple, Union, TypedDict
import dotenv

dotenv.load_dotenv(".env")
PATH_TO_DATADIR = os.getenv("PATH_TO_DATADIR")


class ListeningHistoryEntry(TypedDict):
    track_name: Union[str, None]
    artist_name: Union[str, None]
    played_at: str
    ms_played: int
    episode_name: Union[str, None]
    show_name: Union[str, None]
    track_uri: Union[str, None]
    episode_uri: Union[str, None]


def extract_id_from_uri(uri: str) -> str | None:
    if uri:
        return uri.split(":")[-1]
    return None


# Loop through json files in directory
def extract_listening_history(data_path: str) -> List[ListeningHistoryEntry]:
    dirs = os.listdir(data_path)
    # for filename in dirs:
    #     print(filename)

    if dirs[1].endswith(".json"):
        with open(os.path.join(data_path, dirs[1]), "r") as f:
            data = json.load(f)
        return data


def process_listening_history(data_path: str) -> List[ListeningHistoryEntry]:
    data = extract_listening_history(data_path)
    mapped_fields = {
        "track_name": "master_metadata_track_name",
        "artist_name": "master_metadata_album_artist_name",
        "played_at": "ts",
        "ms_played": "ms_played",
        "episode_name": "episode_name",
        "show_name": "episode_show_name",
        "track_uri": "spotify_track_uri",
        "episode_uri": "spotify_episode_uri",
    }

    extracted_data = []
    seen_entries = set()
    for line in data:
        # Filter out data that doesn't have a timestamp or was listened to for less than a minute
        if line.get("ts") is None or line.get("ms_played") <= 3600:
            continue
        # print(json.dumps(line, separators=(",", ":")))
        entry = {
            new_key: line.get(old_key)
            for new_key, old_key in mapped_fields.items()
            if old_key in line
        }

        unique_contraints = tuple(
            entry.get(key) for key in ["played_at", "ms_played", "track_uri"]
        )

        if unique_contraints not in seen_entries:
            seen_entries.add(unique_contraints)
            extracted_data.append(entry)
    with open("normalized-data/extracted_data.json", "w") as f:
        f.write(json.dumps(extracted_data, indent=2))

    return extracted_data


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


if __name__ == "__main__":
    conn = sqlite3.connect(":memory:")
    initilize_tables(conn)

    data = process_listening_history(PATH_TO_DATADIR)
    for row in data:
        insert_into_db(conn, row)
        # print(json.dumps(row, indent=2))
    # # insert_into_db(conn, data[0])
    cursor = conn.cursor()

    cursor.execute("SELECT * FROM track")
    print(cursor.fetchall())

    cursor.execute("SELECT * FROM episode")
    json.dumps(cursor.fetchall())
    cursor.execute("SELECT * FROM playback")
    json.dumps(cursor.fetchall())

    conn.close()
