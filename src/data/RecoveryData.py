import pyspark.sql.dataframe
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


def total_recovery():
    print("Total US Recovery\n")


def recovery_by_state():
    print("Recovery By State\n")


def vaccination_rate():
    print("Vaccination Rates\n")
