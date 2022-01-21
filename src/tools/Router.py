import sys

import src.App
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
    print("Infection Route")
    infections_menu()


def mortality_route():
    print("Mortality Route")
    deaths_menu()


def recovery_route():
    print("Recovery Route")
    recovery_menu()
