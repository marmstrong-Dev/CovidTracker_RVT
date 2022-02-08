from pyspark.sql.functions import col
from pyspark.sql import DataFrame
from src.tools.Router import spark
from src.tools.Colors import Colors


# Sub Menu For Infections Data
def infections_menu():
    print("""
    MAIN> INFECTIONS DATA>
    1.) Highest Infection Rates By State
    2.) Total US Infections
    3.) Average Infections Per Week
    4.) Return To Main
    """)

    mortality_selector = input(f"{Colors.g}Please Select A Menu Option:{Colors.w}")

    if mortality_selector == "1":
        highest_infections_states()
    elif mortality_selector == "2":
        total_infections()
    elif mortality_selector == "3":
        avg_infections_week_states()
    elif mortality_selector == "4":
        print("Returning To Main")
    else:
        print(f"{Colors.r}Invalid Selection. Please Try Again{Colors.w}")


def create_table():
    print("Creating Infections Table")

    df = spark.con.read.format("csv") \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .load("datasets/csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_confirmed_US.csv")

    return df


def highest_infections_states():
    print("Highest Infections By State\n")

    infections_states_df = create_table()\
        .select(col("Admin2"), col("Province_State"), col("5/29/21"))\
        .withColumnRenamed("5/29/21", "05_29_2021_I")

    infections_states_df.createOrReplaceTempView("StatesRanked")
    spark.con.sql("""
    SELECT Province_State AS States, SUM(05_29_2021_I) AS Infections
    FROM StatesRanked
    WHERE Province_State NOT LIKE("%Princess%")
    GROUP BY Province_State ORDER BY Infections DESC
    """).show(100, True)

    input("Enter Any Key To Return")


def total_infections():
    print("Total US Infections\n")

    overall_df = create_table().select(col("1/21/22")).collect()
    total = 0
    for n in overall_df:
        total += n[0]

    print(total)
    input("\nEnter Any Key To Return")


def avg_infections_week_states():
    print("Average Infections By State\n")

    create_table().select(col("Province_State"), col("1/21/22")) \
        .withColumnRenamed("1/21/22", "Infections") \
        .createOrReplaceTempView("AvgStates")

    avg_infections_week_total()

    spark.con.sql("""
    SELECT Province_State, ROUND((SUM(Infections) / 731) * 7, 2) AS Total_Infections 
    FROM AvgStates
    WHERE Province_State NOT LIKE'%Princess%'
    GROUP BY Province_State
    ORDER BY Total_Infections DESC
    """).show(100, False)

    input("Enter Any Key To Return")


def avg_infections_week_total():
    print("Average Infections Per Week\n")

    avg_df = spark.con.sql("SELECT SUM(Infections) AS US_Total FROM AvgStates").collect()
    print(round((avg_df[0][0] / 731) * 7), 2)
