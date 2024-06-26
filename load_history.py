from config import PATH_TO_DATADIR
from database import insert_into_db, reset_and_connect_db
from normalize import process_listening_history, write_normalized_data_to_json_file


def load_history_data(to_json: bool = False) -> None:
    """Load spotify listening history data into database or write to json file

    Args:
        to_json (bool): Flag indicating whether to create json of data
    """
    data = process_listening_history(PATH_TO_DATADIR)
    conn = reset_and_connect_db()
    for row in data:
        insert_into_db(conn, row)
    conn.close()

    if to_json:
        path_to_json = "normalized-data/extracted_data.json"
        write_normalized_data_to_json_file(data, path_to_json)
