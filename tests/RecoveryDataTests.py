import unittest
from src.data.RecoveryData import create_table


class RecoveryDataTests(unittest.TestCase):
    def test_session(self):
        session = create_table()
        self.assertFalse(session, None)
