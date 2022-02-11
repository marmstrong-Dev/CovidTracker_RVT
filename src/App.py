import src.tools.Router as Route
from src.tools.Colors import Colors


# Welcome Screen
def opener():
    border = ""

    for n in range(0, 15):
        border = border + f"<{Colors.g.value}*|*{Colors.w.value}>"

    print(border)
    print(f"\n  {Colors.b.value}Covid Tracker{Colors.w.value}  \n")
    print(border)


# Display Main Menu And Pass Opt To Router
def main_menu():
    print(f"\n{Colors.b.value}Main Menu{Colors.w.value}")
    print("""
        MAIN>
        1.) Confirmed Infection Data
        2.) Mortality Data
        3.) Recovery Data
        4.) Exit
        """)

    main_selector = input(f"{Colors.g.value}Please Select A Menu Option:{Colors.w.value}")
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
