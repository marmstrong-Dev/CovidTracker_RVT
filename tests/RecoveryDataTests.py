import unittest
from src.data.RecoveryData import create_table, total_recovery, spark


class RecoveryDataTests(unittest.TestCase):
    def test_session(self):
        create_table()
        test_query = spark.con.sql("SELECT * FROM RecoveryInfo").collect()
        self.assertTrue(len(test_query) > 0)

    def test_table_drop(self):
        total_recovery()
        test_drop_query = spark.con.sql("SHOW TABLES").collect()
        self.assertFalse(len(test_drop_query) > 0)
