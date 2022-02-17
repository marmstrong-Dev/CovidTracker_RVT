import unittest
from src.data.MortalityData import create_table, deaths_by_state, spark


class MortalityDataTests(unittest.TestCase):
    def test_session(self):
        session = create_table().collect()
        self.assertTrue(len(session) > 0)

    def test_view_deletion(self):
        deaths_by_state()
        count = spark.con.sql("SHOW TABLES").collect()
        self.assertFalse(len(count) > 0)
