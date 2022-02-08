import pyspark.sql.dataframe
from pyspark.sql.functions import col
from src.tools.Router import spark
from src.tools.Colors import Colors


# Sub Menu For Recovery Data
def recovery_menu():
    print("""
    MAIN> RECOVERY DATA>
    1.) US Total Recovered Patients
    2.) Recovered Patients By State
    3.) US Vaccination Rates
    4.) Return To Main
    """)

    recovery_selector = input(f"{Colors.g}Please Select A Menu Option:{Colors.w}")

    if recovery_selector == "1":
        total_recovery()
    elif recovery_selector == "2":
        recovery_by_state()
    else:
        print(f"{Colors.r}Invalid Selection. Please Try Again{Colors.w}")


def create_table():
    print("Creating Recovery Table")
    df_i = spark.con.read.format("csv") \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .load("datasets/csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_confirmed_US.csv") \
        .select(col("UID"), col("Admin2"), col("Province_State"), col("1/21/22")) \
        .withColumnRenamed("UID", "id_i") \
        .withColumnRenamed("Admin2", "City_i") \
        .withColumnRenamed("Province_State", "State_i") \
        .withColumnRenamed("1/21/22", "1_21_22_i")

    df_d = spark.con.read.format("csv") \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .load("datasets/csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_deaths_US.csv") \
        .select(col("UID"), col("Admin2"), col("Province_State"), col("1/21/22")) \
        .withColumnRenamed("UID", "id_d") \
        .withColumnRenamed("Admin2", "City_d") \
        .withColumnRenamed("Province_State", "State_d") \
        .withColumnRenamed("1/21/22", "1_21_22_d")

    df = df_i.join(df_d, col("id_i") == col("id_d"), "inner").createOrReplaceTempView("RecoveryInfo")
    return df


def total_recovery():
    print("Total US Recovery\n")
    create_table()

    spark.con.sql("""
        SELECT (SUM(1_21_22_i) - SUM(1_21_22_d)) AS NumRecovered,
        ROUND((1 - SUM(1_21_22_d) / SUM(1_21_22_i)) * 100, 2) AS PercentRecovered
        FROM RecoveryInfo""").show()

    input("Enter Any Key To Return")
    spark.con.catalog.dropTempView("RecoveryInfo")


def recovery_by_state():
    print("Recovery By State\n")
    create_table()

    spark.con.sql("""
        SELECT State_d, (SUM(1_21_22_i) - SUM(1_21_22_d)) AS NumRecovered,
        ROUND((1 - SUM(1_21_22_d) / SUM(1_21_22_i)) * 100, 2) AS PercentRecovered
        FROM RecoveryInfo 
        WHERE State_d NOT LIKE('%Princess%')
        GROUP BY State_d ORDER BY PercentRecovered DESC""").show(100, False)

    input("Enter Any Key To Return")
    spark.con.catalog.dropTempView("RecoveryInfo")


def vaccination_rate():
    print("Vaccination Rates\n")
