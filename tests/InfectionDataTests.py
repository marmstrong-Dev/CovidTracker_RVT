import unittest
from src.data.InfectionData import create_table


class InfectionDataTests(unittest.TestCase):
    def test_session(self):
        session = create_table()
        self.assertFalse(session, None)
