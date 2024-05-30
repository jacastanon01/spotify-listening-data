import os
import json
import sqlite3
import dotenv

from database import initilize_tables, insert_into_db
from normalize import process_listening_history

dotenv.load_dotenv(".env")
PATH_TO_DATADIR = os.getenv("PATH_TO_DATADIR", "")


def main():
    conn = sqlite3.connect("data.db")
    initilize_tables(conn)

    data = process_listening_history(PATH_TO_DATADIR)
    for row in data:
        insert_into_db(conn, row)

    conn.close()


if __name__ == "__main__":
    main()
