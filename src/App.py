import tools.Router as Route
from tools.DbCon import DbCon

# Map Of CLI Colors
colors = {
    "W": "\033[0m",  # white (normal)
    "R": "\033[31m",  # red
    "G": "\033[32m",  # green
    "B": "\033[34m"  # blue
}

# Initialize Spark Session
spark = DbCon()


# Welcome Screen
def opener():
    border = ""

    for n in range(0, 15):
        border = border + f"<{colors['G']}*|*{colors['W']}>"

    print(border)
    print(f"\n  {colors['B']}Covid Tracker{colors['W']}  \n")
    print(border)


# Display Main Menu And Pass Opt To Router
def main_menu():
    print(f"\n{colors['B']}Main Menu{colors['W']}")
    print("""
    MAIN>
    1.) Confirmed Infection Data
    2.) Mortality Data
    3.) Recovery Data
    4.) Exit
    """)

    main_selector = input(f"{colors['G']}Please Select A Menu Option:{colors['W']}")
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

    spark.close_session()


if __name__ == "__main__":
    main()
