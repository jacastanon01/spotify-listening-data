import sqlite3

from config import PATH_TO_DATADIR
from database import initialize_tables, insert_into_db
from normalize import process_listening_history


def main():
    conn = sqlite3.connect("data.db")
    initialize_tables(conn)

    data = process_listening_history(PATH_TO_DATADIR)
    for row in data:
        insert_into_db(conn, row)

    conn.close()


if __name__ == "__main__":
    main()
