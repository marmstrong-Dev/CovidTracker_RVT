import os

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
    print("\nMain Menu")


def main():
    opener()
    main_menu()


if __name__ == "__main__":
    main()
