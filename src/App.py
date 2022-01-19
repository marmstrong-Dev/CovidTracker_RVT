import os


def opener():
    border = ""

    for n in range(0, 15):
        border = border + "<|>"

    print(border)
    print("\n  Covid Tracker  \n")
    print(border)


def main():
    opener()


if __name__ == "__main__":
    main()
