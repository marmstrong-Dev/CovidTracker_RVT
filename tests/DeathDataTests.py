import unittest
from src.data.MortalityData import create_table


class DeathDataTests(unittest.TestCase):
    def test_session(self):
        session = create_table()
        self.assertFalse(session, None)
