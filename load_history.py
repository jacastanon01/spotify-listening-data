import os
import sqlite3

from config import PATH_TO_DATADIR
from database import initialize_db, insert_into_db, reset_and_connect_db
from normalize import process_listening_history, write_normalized_data_to_json_file


def load_history_data() -> None:
    conn = reset_and_connect_db()

    path_to_json = "normalized-data/extracted_data.json"
    data = process_listening_history(PATH_TO_DATADIR)
    write_normalized_data_to_json_file(data, path_to_json)
    for row in data:
        insert_into_db(conn, row)

    conn.close()
