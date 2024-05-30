from typing import TypedDict, Union, List
import os
import json


class IListeningHistoryEntry(TypedDict):
    track_name: Union[str, None]
    artist_name: Union[str, None]
    played_at: str
    ms_played: int
    episode_name: Union[str, None]
    show_name: Union[str, None]
    track_uri: Union[str, None]
    episode_uri: Union[str, None]


def extract_id_from_uri(uri: str) -> str:
    return uri.split(":")[-1]


# Loop through json files in directory
def extract_listening_history(data_path: str) -> List[IListeningHistoryEntry]:
    try:
        dirs = os.listdir(data_path)
        # for filename in dirs:
        #     print(filename)
        data = []
        if dirs[1].endswith(".json"):
            with open(os.path.join(data_path, dirs[1]), "r") as f:
                data = json.load(f)
        return data
    except:
        raise ValueError("No data found in directory.")


def process_listening_history(data_path: str) -> List[IListeningHistoryEntry]:
    data = extract_listening_history(data_path)
    if not data:
        raise
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
