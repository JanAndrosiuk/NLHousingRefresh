from src.refresh_yourhouse import *
import sys


def main():

    app = HousingRefresh()
    app.check_for_changes()

    sys.exit("Aborting.")


if __name__ == "__main__":
    main()
