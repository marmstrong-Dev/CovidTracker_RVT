from src.data.InfectionData import infections_menu
from src.data.RecoveryData import recovery_menu
from src.data.MortalityData import mortality_menu
from src.tools.Colors import Colors


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
        print(f"{Colors.r.value}Invalid Selection. Please Try Again.{Colors.w.value}")


# Sub-Route For Infections Data
def infection_route():
    print(f"\n{Colors.b.value}Infection Menu{Colors.w.value}")
    infections_menu()


# Sub-Route For Mortality Data
def mortality_route():
    print(f"\n{Colors.b.value}Mortality Menu{Colors.w.value}")
    mortality_menu()


# Sub-Route For Recovery Data
def recovery_route():
    print(f"\n{Colors.b.value}Recovery Menu{Colors.w.value}")
    recovery_menu()
