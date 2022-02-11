from pyspark.sql.functions import col
from src.tools.DbCon import DbCon
from src.tools.Colors import Colors

spark = DbCon()


# Sub Menu For Mortality Data
def mortality_menu():
    print("""
    MAIN> MORTALITY DATA>
    1.) Mortality Percentages By State
    2.) Mortality By State
    3.) US Average Weekly Deaths
    4.) Return To Main
    """)

    death_selector = input(f"{Colors.g.value}Please Select A Menu Option:{Colors.w.value}")

    if death_selector == "1":
        mortality_rates_state()
    elif death_selector == "2":
        deaths_by_state()
    elif death_selector == "3":
        average_weekly_deaths()
    else:
        print(f"{Colors.r.value}Invalid Selection. Please Try Again{Colors.w.value}")


# Create And Return DataFrame For Mortality Data
def create_table():
    print("Creating Mortality Table")

    try:
        df = spark.con.read.format("csv") \
            .option("header", "true") \
            .option("inferSchema", "true") \
            .load("datasets/csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_deaths_US.csv")

        return df
    except:
        print("File Not Found")
        return None


# Print Overall Mortality By State
def deaths_by_state():
    print("Mortality Numbers By State\n")

    create_table() \
        .select(col("Province_State"), col("1/21/22")) \
        .withColumnRenamed("1/21/22", "1_21_22") \
        .createOrReplaceTempView("MortalityList")

    spark.con.sql("""
        SELECT Province_State, SUM(1_21_22) AS Infections
        FROM MortalityList
        WHERE Province_State NOT LIKE("%Princess%")
        GROUP BY Province_State
        ORDER BY Infections DESC""").show(100, False)

    input("Enter Any Key To Return")
    spark.con.catalog.dropTempView("MortalityList")


# Print Mortality Count By State
def mortality_rates_state():
    print("Mortality Rates By State\n")

    create_table() \
        .withColumnRenamed("1/21/22", "1_21_22_d") \
        .createOrReplaceTempView("DeathList")

    spark.con.read.format("csv") \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .load("datasets/csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_confirmed_US.csv") \
        .select(col("UID"), col("1/21/22")) \
        .withColumnRenamed("1/21/22", "1_21_22_i") \
        .createOrReplaceTempView("InfectionList")

    spark.con.sql("""
        SELECT DeathList.Province_State, ROUND((SUM(1_21_22_d) / SUM(1_21_22_i))*100, 2) AS MortalityRate
        FROM DeathList LEFT JOIN InfectionList
        ON DeathList.UID = InfectionList.UID
        WHERE DeathList.Province_State NOT LIKE("%Princess%")
        GROUP BY DeathList.Province_State
        ORDER BY MortalityRate DESC""").show(100, False)

    input("Enter Any Key To Return")
    spark.con.catalog.dropTempView("DeathList")
    spark.con.catalog.dropTempView("InfectionList")


# Print Average Weekly Deaths Per State
def average_weekly_deaths():
    print("US Average Deaths Per Week\n")
    create_table() \
        .select(col("Province_State"), col("1/21/22")) \
        .withColumnRenamed("1/21/22", "1_21_22") \
        .createOrReplaceTempView("AvgList")

    spark.con.sql("SELECT ROUND((SUM(1_21_22) / 731) * 7, 2) AS TotalWeeklyDeaths FROM AvgList").show(5, False)
    spark.con.sql("""
        SELECT Province_State, ROUND((SUM(1_21_22) / 731) * 7, 2) AS WeeklyDeaths
        FROM AvgList
        WHERE Province_State NOT LIKE("%Princess%")
        GROUP BY Province_State
        ORDER BY WeeklyInfections DESC""").show(100, False)

    input("Enter Any Key To Return")
    spark.con.catalog.dropTempView("AvgList")

