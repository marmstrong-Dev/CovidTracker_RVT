import enum


# Map Of CLI Colors
class Colors(enum.Enum):
    r = "\033[31m"  # red
    g = "\033[32m"  # green
    b = "\033[34m"  # blue
    w = "\033[0m"  # white (normal)
