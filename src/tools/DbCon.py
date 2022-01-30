from pyspark.sql import SparkSession


# Class For Establishing / Closing Spark Session
class DbCon:
    def __init__(self):
        self.con = self.create_session()

    @staticmethod
    def create_session():
        new_session = SparkSession.builder \
            .master("local") \
            .appName("Covid Tracker") \
            .enableHiveSupport() \
            .getOrCreate()
        new_session.sparkContext.setLogLevel('ERROR')

        return new_session

    def close_session(self):
        self.con.stop()
