import sqlite3
import unittest

from database import initialize_tables, insert_into_db
from normalize import IListeningHistoryEntry


class TestInsertIntoDB(unittest.TestCase):
    def setUp(self):
        self.connection = sqlite3.connect(":memory:")
        self.cursor = self.connection.cursor()
        initialize_tables(self.connection)

    def tearDown(self):
        self.connection.close()

    def test_insert_track_entry(self):
        entry: IListeningHistoryEntry = {
            "track_uri": "spotify:track:123",
            "track_name": "Test Track",
            "artist_name": "Test Artist",
            "played_at": "2023-01-01T12:00:00Z",
            "ms_played": 180000,
            "episode_name": None,
            "show_name": None,
            "episode_uri": None,
        }
        insert_into_db(self.connection, entry)
        self.connection.commit()

        self.cursor.execute("SELECT * FROM track")
        track = self.cursor.fetchone()

        self.assertIsNotNone(track)
        self.assertEqual(track[1], "spotify:track:123")
        self.assertEqual(track[2], "Test Track")
        self.assertEqual(track[3], "Test Artist")

        self.cursor.execute("SELECT * FROM playback")
        playback = self.cursor.fetchone()

        self.assertIsNotNone(playback)
        self.assertEqual(playback[1], "2023-01-01T12:00:00Z")
        self.assertEqual(playback[2], 180000)

        self.cursor.execute(
            "SELECT * FROM track_activity WHERE track_id = ?", (track[0],)
        )
        activity = self.cursor.fetchone()
        self.assertIsNotNone(activity)
        self.assertEqual(activity[1], "123")

    def test_insert_episode_entry(self):
        entry: IListeningHistoryEntry = {
            "episode_uri": "spotify:episode:456",
            "episode_name": "Test Episode",
            "show_name": "Test Show",
            "played_at": "2023-01-02T12:00:00Z",
            "ms_played": 180000,
            "track_name": None,
            "artist_name": None,
            "track_uri": None,
        }

        insert_into_db(self.connection, entry)
        self.connection.commit()

        self.cursor.execute("SELECT * FROM episode")
        episode = self.cursor.fetchone()

        self.assertIsNotNone(episode)
        self.assertEqual(episode[1], "spotify:episode:456")
        self.assertEqual(episode[2], "Test Episode")
        self.assertEqual(episode[3], "Test Show")

        self.cursor.execute("SELECT * FROM playback")
        playback = self.cursor.fetchone()

        self.assertIsNotNone(playback)
        self.assertEqual(playback[1], "2023-01-02T12:00:00Z")
        self.assertEqual(playback[2], 180000)

        self.cursor.execute(
            "SELECT * FROM episode_activity WHERE episode_id = ?", (episode[0],)
        )
        activity = self.cursor.fetchone()
        self.assertIsNotNone(activity)
        self.assertEqual(activity[1], "456")


if __name__ == "__main__":
    unittest.main()
