import dotenv
import os

# ENVIRONMENT
dotenv.load_dotenv(".env")
PATH_TO_DATADIR = os.getenv("PATH_TO_DATADIR", "")

# GLOBAL
SQL_QUERY_PATHS = [
    "./create_episode_activity.sql",
    "./create_episode_table.sql",
    "./create_playback_table.sql",
    "./create_track_activity_table.sql",
    "./create_track_table.sql",
]
