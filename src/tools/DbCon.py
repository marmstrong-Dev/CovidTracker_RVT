import pyspark


def create_session():
    con = pyspark.sql.SparkSession.builder\
        .config("spark.master", "local")\
        .appName("Covid Tracker")\
        .getOrCreate()

    return con


def close_session(session: pyspark.sql.SparkSession):
    session.stop()
