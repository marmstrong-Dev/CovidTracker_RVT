import src.tools.Router as Route
from src.tools.DbCon import DbCon
from src.tools.Colors import Colors

"""colors = {
    "W": "033[0m",  # white (normal)
    "R": "033[31m",  # red
    "G": "033[32m",  # green
    "B": "033[34m"  # blue
}"""


# spark = DbCon()


# Welcome Screen
def opener():
    border = ""

    for n in range(0, 15):
        border = border + f"<{Colors.g}*|*{Colors.w}>"

    print(border)
    print(f"\n  {Colors.b}Covid Tracker{Colors.w}  \n")
    print(border)


# Display Main Menu And Pass Opt To Router
def main_menu():
    print(f"\n{Colors.b}Main Menu{Colors.w}")
    print("""
        MAIN>
        1.) Confirmed Infection Data
        2.) Mortality Data
        3.) Recovery Data
        4.) Exit
        """)

    main_selector = input(f"{Colors.g}Please Select A Menu Option:{Colors.w}")
    Route.route_mapper(main_selector)

    if main_selector == "4":
        return False
    else:
        return True


def main():
    opener()
    occupied = True

    while occupied:
        occupied = main_menu()


if __name__ == "__main__":
    main()
