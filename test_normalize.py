from typing import Any, Callable, Dict, List
import unittest
from unittest.mock import call, patch, mock_open, MagicMock
import json

from normalize import (
    IListeningHistoryEntry,
    process_listening_history,
    write_normalized_data_to_json_file,
)

MOCK_DATA: List[Dict[str, Any]] = [
    {
        "ts": "2020-01-22T19:39:58Z",
        "username": "jacastanon01",
        "platform": "web_player windows 10;firefox 72.0;desktop",
        "ms_played": 4711,
        "conn_country": "ZZ",
        "ip_addr_decrypted": "168.137.100.21",
        "user_agent_decrypted": "Mozilla%2F5.0%20(Windows%20NT%2010.0;%20Win64;%20x64;%20rv:72.0)%20Gecko%2F20100101%20Firefox%2F72.0",
        "master_metadata_track_name": "Till I Collapse",
        "master_metadata_album_artist_name": "Eminem",
        "master_metadata_album_album_name": "The Eminem Show",
        "spotify_track_uri": "spotify:track:4xkOaSrkexMciUUogZKVTS",
        "episode_name": None,
        "episode_show_name": None,
        "spotify_episode_uri": None,
        "reason_start": "trackdone",
        "reason_end": "endplay",
        "shuffle": False,
        "skipped": None,
        "offline": False,
        "offline_timestamp": 0,
        "incognito_mode": False,
    }
]


# def patch_process_listening_data(mock_data: Any, mock_files: List[str]) -> Callable:
#     def decorator(test_func: Callable) -> Callable:
#         @patch("os.listdir")
#         @patch(
#             "builtins.open",
#             new_callable=mock_open,
#             read_data=json.dumps(mock_data),
#         )
#         def wrapper(mock_open: MagicMock, mock_listdir: MagicMock, *args, **kwargs):
#             mock_listdir.return_value = mock_files
#             return test_func(mock_open, mock_listdir, *args, **kwargs)

#         return wrapper

#     return decorator


class TestProcessListeningHistory(unittest.TestCase):
    @patch("os.listdir")
    @patch(
        "builtins.open",
        new_callable=mock_open,
        read_data=json.dumps(MOCK_DATA),
    )
    def test_process_listening_history(self, mock_open, mock_listdir):
        mock_listdir.return_value = ["ignore.txt", "test_data.json"]
        data_path = "/fake/path"
        result = process_listening_history(data_path)

        expected_output = [
            {
                "track_name": "Till I Collapse",
                "artist_name": "Eminem",
                "played_at": "2020-01-22T19:39:58Z",
                "ms_played": 4711,
                "episode_name": None,
                "show_name": None,
                "track_uri": "spotify:track:4xkOaSrkexMciUUogZKVTS",
                "episode_uri": None,
            }
        ]

        self.assertEqual(result, expected_output)
        mock_open.assert_any_call("/fake/path/test_data.json", "r")

    @patch(
        "builtins.open",
        new_callable=mock_open,
    )
    @patch("json.dump")
    def test_write_data_to_json_file(self, mock_json_dump, mock_open_func):
        test_data: List[IListeningHistoryEntry] = [
            {
                "track_name": "Till I Collapse",
                "artist_name": "Eminem",
                "played_at": "2020-01-22T19:39:58Z",
                "ms_played": 4711,
                "episode_name": None,
                "show_name": None,
                "track_uri": "spotify:track:4xkOaSrkexMciUUogZKVTS",
                "episode_uri": None,
            }
        ]

        # Call the function with the test data
        write_normalized_data_to_json_file(test_data)

        # Assert that the file was opened with the correct path
        mock_open_func.assert_called_once_with(
            "normalized-data/extracted_data.json", "w"
        )
        mock_json_dump.assert_called_once_with(test_data, mock_open_func(), indent=2)

        expected_json_data = '[\n  {\n    "track_name": "Till I Collapse",\n    "artist_name": "Eminem",\n    "played_at": "2020-01-22T19:39:58Z",\n    "ms_played": 4711,\n    "episode_name": null,\n    "show_name": null,\n    "track_uri": "spotify:track:4xkOaSrkexMciUUogZKVTS",\n    "episode_uri": null\n  }\n]'

        # expected_calls = [call(test_data, mock_open_func(), indent=2)]
        # self.assertEqual(mock_json_dump.call_args_list, expected_calls)
        # expected_calls = [call(test_data, mock_open_func(), indent=2)]
        # self.assertEqual(mock_json_dump.call_args_list, expected_calls)
        # mock_open_func().write.assert_called_once_with(expected_json_data)

        # # Expected output file path
        # expected_path = "normalized-data/extracted_data.json"

        # # Call the function
        # # with patch("builtins.open", mock_open, create=True):
        # write_normalized_data_to_json_file(data)
        # print(mock_open.mock_open.call_args_list)

        # # Assertions
        # mock_remove.assert_called_once_with(
        #     expected_path
        # )  # Check if os.remove is called with the expected path
        # # mock_open.assert_called_once_with(expected_path, "w")

        # # with patch("builtins.open", mock_open, create=True):
        # #     process_listening_history("/fake/path")
        # # mock_open.assert_any_call("normalized-data/extracted_data.json", "w")

        # # Check the data written to the output file
        # handle = mock_open()
        # # print(handle.write.mock_calls)
        # handle.write.assert_any_call(json.dumps(data, indent=2))

    @patch("os.listdir")
    @patch(
        "builtins.open",
        new_callable=mock_open,
        read_data=json.dumps("invalid json"),
    )
    def test_invalid_json_file(self, mock_open, mock_listdir):
        data_path = "/fake/path"
        mock_listdir.return_value = ["", "blah.txt"]
        with self.assertRaises(ValueError):
            process_listening_history(data_path)

    @patch("os.listdir")
    @patch(
        "builtins.open",
        new_callable=mock_open,
        read_data=json.dumps([{"ts": "2020-01-22T19:39:58Z", "ms_played": 2711}]),
    )
    def test_missing_keys(self, mock_open, mock_listdir):
        incomplete_data = [{"ts": "2020-01-22T19:39:58Z", "ms_played": 2711}]
        mock_open.return_value.read_data = json.dumps(incomplete_data)
        mock_listdir.return_value = ["ignore.txt", "incomplete.json"]
        data_path = "/fake/path"
        result = process_listening_history(data_path)
        expected_output = []
        self.assertEqual(result, expected_output)

    @patch("os.listdir")
    @patch(
        "builtins.open",
        new_callable=mock_open,
        read_data=json.dumps(MOCK_DATA * 2),
    )
    def test_duplicate_entries(self, mock_open, mock_listdir):
        mock_listdir.return_value = ["ignore.txt", "duplicate.json"]
        data_path = "/fake/path"
        result = process_listening_history(data_path)
        self.assertEqual(len(result), 1)

    @patch("os.listdir")
    @patch(
        "builtins.open",
        new_callable=mock_open,
        read_data=json.dumps([dict(MOCK_DATA[0], ms_played=1000)]),
    )
    def test_entries_below_threshold(self, mock_open, mock_listdir):
        # low_played_data = [dict(MOCK_DATA[0], ms_played=1000)]
        # mock_open.return_value.read_data = json.dumps(low_played_data)
        mock_listdir.return_value = ["ignore.txt", "low_played.json"]
        data_path = "/fake/path"
        result = process_listening_history(data_path)
        self.assertEqual(result, [])
        # mock_open.assert_called_once_with("/fake/path/low_played.json", "r")


if __name__ == "__main__":
    unittest.main()
