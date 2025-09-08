import argparse
import scrap_raw_applications
import scrap_ncbo
import scrap_completion

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--job", required=True, choices=["scrap_raw_applications", "scrap_ncbo","scrap_completion"])
    args = parser.parse_args()

    if args.job == "scrap_raw_applications":
        scrap_raw_applications.run()
    elif args.job == "scrap_ncbo":
        scrap_ncbo.run()
    elif args.job == "scrap_completion":
        scrap_completion.run()
