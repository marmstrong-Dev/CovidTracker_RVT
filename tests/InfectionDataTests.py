import unittest
from src.data.InfectionData import create_table, total_infections


class InfectionDataTests(unittest.TestCase):
    def test_session(self):
        session = create_table().collect()
        self.assertTrue(len(session) > 0)

    def test_total_infections(self):
        test_total = total_infections()
        self.assertTrue(test_total > 0)
