import pyspark.sql.dataframe
from pyspark.sql.functions import col
from src.App import colors, spark


# Sub Menu For Recovery Data
def recovery_menu():
    print("""
    MAIN> RECOVERY DATA>
    1.) US Total Recovered Patients
    2.) Recovered Patients By State
    3.) US Vaccination Rates
    4.) Return To Main
    """)

    recovery_selector = input(f"{colors['G']}Please Select A Menu Option:{colors['W']}")

    if recovery_selector == "1":
        total_recovery()
    elif recovery_selector == "2":
        recovery_by_state()
    else:
        print(f"{colors['R']}Invalid Selection. Please Try Again{colors['W']}")


def create_table():
    print("Creating Infections Table")
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

    spark.con.sql("SELECT (SUM(1_21_22_i) - SUM(1_21_22_d)) AS Recovered FROM RecoveryInfo").show()


def recovery_by_state():
    print("Recovery By State\n")


def vaccination_rate():
    print("Vaccination Rates\n")
