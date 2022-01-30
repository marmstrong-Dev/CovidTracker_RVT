from src.App import colors
from src.data.InfectionData import infections_menu
from src.data.RecoveryData import recovery_menu
from src.data.MortalityData import mortality_menu


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
    else:
        print(f"{colors['R']}Invalid Selection. Please Try Again.{colors['W']}")


# Sub-Route For Infections Data
def infection_route():
    print(f"\n{colors['B']}Infection Menu{colors['W']}")
    infections_menu()


# Sub-Route For Mortality Data
def mortality_route():
    print(f"\n{colors['B']}Mortality Menu{colors['W']}")
    mortality_menu()


# Sub-Route For Recovery Data
def recovery_route():
    print(f"\n{colors['B']}Recovery Menu{colors['W']}")
    recovery_menu()
