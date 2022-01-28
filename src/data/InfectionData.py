import pyspark.sql.dataframe
from src.App import colors, spark


# Sub Menu For Infections Data
def infections_menu():
    print("""
    MAIN> INFECTIONS DATA>
    1.) Highest Infection Rates By State
    2.) Total US Infections
    3.) Average Infections Per Week
    4.) Return To Main
    """)

    mortality_selector = input("Please Select An Option:")

    if mortality_selector == "1":
        highest_infections()
    elif mortality_selector == "2":
        total_infections()
    elif mortality_selector == "3":
        avg_infections_week()
    else:
        print(f"{colors['R']}Invalid Selection. Please Try Again{colors['W']}")


def highest_infections():
    print("Highest Infections By State\n")


def total_infections():
    print("Total US Infections\n")


def avg_infections_week():
    print("Average Infections Per Week\n")
