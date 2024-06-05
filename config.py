import dotenv
import os

# ENVIRONMENT
dotenv.load_dotenv(".env")
PATH_TO_DATADIR = os.getenv("PATH_TO_DATADIR", "")

# GLOBAL
SQL_QUERY_PATHS = [
    "queries/create/create_episode_activity.sql",
    "queries/create/create_episode_table.sql",
    "queries/create/create_playback_table.sql",
    "queries/create/create_track_activity_table.sql",
    "queries/create/create_track_table.sql",
]
