from pyspark.sql.functions import col
from src.tools.DbCon import DbCon
from src.tools.Colors import Colors

spark = DbCon()


# Sub Menu For Infections Data
def infections_menu():
    print("""
    MAIN> INFECTIONS DATA>
    1.) Highest Infection Rates By State
    2.) Total US Infections
    3.) Average Infections Per Week
    4.) Return To Main
    """)

    mortality_selector = input(f"{Colors.g.value}Please Select A Menu Option:{Colors.w.value}")

    if mortality_selector == "1":
        highest_infections_states()
    elif mortality_selector == "2":
        total_infections()
    elif mortality_selector == "3":
        avg_infections_week_states()
    elif mortality_selector == "4":
        print("Returning To Main")
    else:
        print(f"{Colors.r.value}Invalid Selection. Please Try Again{Colors.w.value}")


# Create Initial DataFrame For Querying
def create_table():
    print("Creating Infections Table")

    try:
        df = spark.con.read.format("csv") \
            .option("header", "true") \
            .option("inferSchema", "true") \
            .load("hdfs://localhost:9000/user/narma/covid/data/time_series_covid19_confirmed_US.csv")

        return df
    except:
        print("File Not Found")
        return None


# Pull Highest Infections Numbers Per State
def highest_infections_states():
    print("Highest Infections By State\n")

    infections_states_df = create_table()\
        .select(col("Admin2"), col("Province_State"), col("1/21/22"))\
        .withColumnRenamed("1/21/22", "01_21_2022_I")

    infections_states_df.createOrReplaceTempView("StatesRanked")
    spark.con.sql("""
    SELECT Province_State AS States, SUM(01_21_2022_I) AS Infections
    FROM StatesRanked
    WHERE Province_State NOT LIKE("%Princess%")
    GROUP BY Province_State ORDER BY Infections DESC
    """).show(100, True)

    input("Enter Any Key To Return")


# Pull Overall US Infections
def total_infections():
    print("Total US Infections\n")

    overall_df = create_table().select(col("1/21/22")).collect()
    total = 0
    for n in overall_df:
        total += n[0]

    print(f"\n{Colors.b.value}Total Infections To Date: {Colors.w.value}")
    print("{:,}".format(total))
    input("\nEnter Any Key To Return")

    return total


# Pull Average Weekly Infections Per State
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


# Pull Average Weekly Infections For Whole USA
def avg_infections_week_total():
    print("Average Infections Per Week\n")

    avg_df = spark.con.sql("SELECT SUM(Infections) AS US_Total FROM AvgStates").collect()
    print(round((avg_df[0][0] / 731) * 7), 2)
