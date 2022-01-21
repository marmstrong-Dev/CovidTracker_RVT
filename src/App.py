import os
import tools.Router as Route

colors = {
    "W": "\033[0m",  # white (normal)
    "R": "\033[31m",  # red
    "G": "\033[32m",  # green
    "B": "\033[34m"  # blue
}


def opener():
    border = ""

    for n in range(0, 15):
        border = border + "<|>"

    print(border)
    print(f"\n  {colors['B']}Covid Tracker{colors['W']}  \n")
    print(border)


def main_menu():
    print(f"\n{colors['B']}Main Menu{colors['W']}")
    print("""
    1.) Confirmed Infection Data
    2.) Mortality Data
    3.) Recovery Data
    4.) Exit
    """)

    main_selector = input("Please Select A Menu Option: ")
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
