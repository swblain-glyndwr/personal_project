import argparse
from datetime import datetime
from scripts.table_operations import create_tables, init_starting_tables
from scripts import parse_attributes, parse_theme_mapping, build_markov_chain


def main(sample):
    """Main function to set up dev tables."""
    print("Creating tables...")
    create_tables.main(
        JOB_ENV="DEV", CLIENT="next_uk", LOG_LEVEL="INFO", DROP_TABLES=False
    )

    if sample:
        print("Running in sample mode...")
        init_starting_tables.main(CLIENT="next_uk", LOG_LEVEL="INFO")
    else:
        print("Running in standard mode...")
        today = datetime.now().strftime("%Y-%m-%d")
        print(f"Using date {today} for populating tables")

        # print("Running parse_attributes...")
        parse_attributes.main(
            JOB_ENV="DEV",
            CLIENT="next_uk",
            LOG_LEVEL="INFO",
            REFRESH_ATTRIBUTES_DATE=today,
        )

        print("Running parse_theme_mapping...")
        parse_theme_mapping.main(
            JOB_ENV="DEV",
            CLIENT="next_uk",
            LOG_LEVEL="INFO",
            REFRESH_THEMES_DATE=today,
        )

        print("Running build_markov_chain...")
        build_markov_chain.main(
            JOB_ENV="DEV", CLIENT="next_uk", LOG_LEVEL="INFO"
        )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Set up development tables.")
    mode_group = parser.add_mutually_exclusive_group()
    mode_group.add_argument(
        "--sample",
        dest="sample",
        action="store_true",
        help="Run in sample mode with a small amount of data (default)",
    )
    mode_group.add_argument(
        "--standard",
        dest="sample",
        action="store_false",
        help="Run in standard mode with full processing",
    )
    parser.set_defaults(sample=True)
    args = parser.parse_args()
    main(args.sample)
