"""
Script to build the SQLite database used by the arboviroses backend.

This script reads the weekly arboviroses case data from
``data/sinan_arboviroses_data.csv``, aggregates case counts by city and
week, and stores the results in a table called ``weekly_cases`` within
``data/arboviroses.db``.  The resulting database can be queried by
``backend/app.py`` to serve data to the dashboard.

Usage:

.. code-block:: bash

   cd backend
   python scripts/build_database.py

Ensure that the ``pandas`` library is installed in your environment.
"""

import argparse
import pandas as pd
import sqlite3
import os
from pathlib import Path


def build_db(
    sinan_path: Path,
    db_path: Path,
    date_col: str = "date",
    city_col: str = "municipality",
) -> None:
    """Construct or rebuild the arboviroses SQLite database.

    This function attempts to read a CSV file containing arboviroses case
    data and aggregates it by week and municipality.  If the input file is
    missing or cannot be parsed, an empty database with the appropriate
    schema will still be created to satisfy the backend contract.

    Parameters
    ----------
    sinan_path: Path
        Path to the SINAN arboviroses CSV file.  Must include the
        ``date_col`` and ``total_cases`` columns.  If the file is absent,
        an empty database is created.
    db_path: Path
        Destination for the SQLite database.  Any existing file is
        overwritten.
    date_col: str, optional
        Name of the date column in ``sinan_path``.
    city_col: str, optional
        Name of the municipality column in ``sinan_path``.
    """
    # Remove any existing database file
    if db_path.exists():
        db_path.unlink()
    conn = sqlite3.connect(db_path)
    try:
        # Create the weekly_cases table schema regardless of whether we have data
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS weekly_cases (
                date TEXT NOT NULL,
                total_cases INTEGER,
                city TEXT
            )
            """
        )
        # Attempt to load the dataset.  Catch exceptions and skip data loading
        if sinan_path.exists():
            try:
                # Attempt to automatically detect delimiter and parse dates
                df = pd.read_csv(
                    sinan_path,
                    parse_dates=[date_col],
                    engine="python",
                )
                # If required columns are missing, skip loading
                required_cols = {date_col, city_col, "total_cases"}
                if required_cols.issubset(df.columns):
                    for city in df[city_col].unique():
                        city_df = df[df[city_col] == city].copy()
                        city_df.set_index(date_col, inplace=True)
                        weekly = city_df.resample("W-SUN").agg({"total_cases": "sum"})
                        weekly["city"] = city
                        weekly.reset_index(inplace=True)
                        # Convert date to ISO string for SQLite
                        weekly[date_col] = weekly[date_col].dt.date.astype(str)
                        weekly.to_sql("weekly_cases", conn, if_exists="append", index=False)
                    conn.commit()
                else:
                    print(
                        f"WARNING: Columns {required_cols} not found in {sinan_path}, skipping data loading."
                    )
            except Exception as exc:
                print(
                    f"ERROR: Failed to read {sinan_path}: {exc}. Creating empty database without data."
                )
        else:
            print(f"WARNING: Input file {sinan_path} does not exist. Creating empty database.")
        # No data inserted results in empty weekly_cases table
    finally:
        conn.close()


def main() -> None:
    """Entry point for the build script.

    Command line arguments allow overriding the input CSV and output
    database paths.  When invoked without arguments, it defaults to
    ``backend/data/sinan_arboviroses_data.csv`` and
    ``backend/data/arboviroses.db`` relative to the script location.
    """
    parser = argparse.ArgumentParser(description="Build the arboviroses SQLite database.")
    parser.add_argument(
        "-i",
        "--input",
        type=str,
        help="Path to the SINAN arboviroses CSV file. Defaults to backend/data/sinan_arboviroses_data.csv.",
    )
    parser.add_argument(
        "-o",
        "--output",
        type=str,
        help="Path where the SQLite database will be created. Defaults to backend/data/arboviroses.db.",
    )
    args = parser.parse_args()
    # Determine base directory relative to this script
    script_dir = Path(__file__).resolve().parent
    base_dir = script_dir.parent
    data_dir = base_dir / "data"
    # Resolve input and output paths
    sinan_path = Path(args.input) if args.input else data_dir / "sinan_arboviroses_data.csv"
    db_path = Path(args.output) if args.output else data_dir / "arboviroses.db"
    print(f"Building database from {sinan_path} -> {db_path}")
    build_db(sinan_path, db_path)
    print("Database build complete.")


if __name__ == "__main__":
    main()