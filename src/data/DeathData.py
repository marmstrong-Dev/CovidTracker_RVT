import pyspark.sql.dataframe
from src.App import colors, spark


# Sub Menu For Mortality Data
def deaths_menu():
    print("""
    MAIN> MORTALITY DATA>
    1.) Mortality Percentages By State
    2.) Casualties By State
    3.) US Average Weekly Deaths
    4.) Return To Main
    """)

    death_selector = input("Please Select An Option:")

    if death_selector == "1":
        mortality_by_state()
    elif death_selector == "2":
        deaths_by_state()
    elif death_selector == "3":
        average_weekly_deaths()
    else:
        print(f"{colors['R']}Invalid Selection. Please Try Again{colors['W']}")


def deaths_by_state():
    print("Mortality Numbers By State\n")

    df = spark.read.csv("datasets/csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_deaths_US.csv")
    df.show()


def mortality_by_state():
    print("Mortality Rates By State\n")


def average_weekly_deaths():
    print("US Average Deaths Per Week\n")
