from src.data.InfectionData import infections_menu
from src.data.RecoveryData import recovery_menu
from src.data.MortalityData import mortality_menu
from src.tools.Colors import Colors
from src.tools.DbCon import DbCon

# Initialize Spark Session
spark = DbCon()


# Main Router Passes Opts To Sub-Routes
def route_mapper(selector):
    if selector == "1":
        infection_route()
    elif selector == "2":
        mortality_route()
    elif selector == "3":
        recovery_route()
    elif selector == "4":
        print("Goodbye")
        spark.close_session()
    else:
        print(f"{Colors.r}Invalid Selection. Please Try Again.{Colors.w}")


# Sub-Route For Infections Data
def infection_route():
    print(f"\n{Colors.b}Infection Menu{Colors.w}")
    infections_menu()


# Sub-Route For Mortality Data
def mortality_route():
    print(f"\n{Colors.b}Mortality Menu{Colors.w}")
    mortality_menu()


# Sub-Route For Recovery Data
def recovery_route():
    print(f"\n{Colors.b}Recovery Menu{Colors.w}")
    recovery_menu()
