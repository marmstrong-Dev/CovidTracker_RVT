from src.App import colors
from src.data.InfectionData import infections_menu
from src.data.RecoveryData import recovery_menu
from src.data.DeathData import deaths_menu


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


def infection_route():
    print(f"\n{colors['B']}Infection Route{colors['W']}")
    infections_menu()


def mortality_route():
    print(f"\n{colors['B']}Mortality Route{colors['W']}")
    deaths_menu()


def recovery_route():
    print(f"\n{colors['B']}Recovery Route{colors['W']}")
    recovery_menu()
