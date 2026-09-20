from pathlib import Path
import csv

import pandas as pd


BASE_DIR = Path(__file__).resolve().parents[2]

FACT_SALES_DIR = BASE_DIR / "data" / "source" / "fact_sales"
MASTER_FILE = BASE_DIR / "data" / "source" / "KopDes_MasterData.xlsx"


def profile_csv_files():
    csv_files = sorted(FACT_SALES_DIR.glob("*.csv"))

    print("=" * 70)
    print("FACT SALES CSV PROFILE")
    print("=" * 70)

    reference_columns = None

    for file_path in csv_files:
        with file_path.open("r", encoding="utf-8-sig", newline="") as file:
            reader = csv.reader(file)

            header = next(reader)

            row_count = sum(1 for _ in reader)

        file_size_mb = file_path.stat().st_size / (1024 * 1024)

        if reference_columns is None:
            reference_columns = header

        same_schema = header == reference_columns

        print(f"\nFile        : {file_path.name}")
        print(f"Rows        : {row_count:,}")
        print(f"Size        : {file_size_mb:.2f} MB")
        print(f"Columns     : {len(header)}")
        print(f"Same Schema : {same_schema}")
        print(f"Column List : {header}")


def profile_master_data():
    print("\n")
    print("=" * 70)
    print("MASTER DATA PROFILE")
    print("=" * 70)

    workbook = pd.ExcelFile(MASTER_FILE)

    print(f"\nWorkbook : {MASTER_FILE.name}")
    print(f"Sheets   : {workbook.sheet_names}")

    ignored_sheets = {"README"}

    for sheet_name in workbook.sheet_names:

        if sheet_name.strip().upper() in ignored_sheets:
            print(f"\nSheet   : {sheet_name}")
            print("Status  : SKIPPED (documentation sheet)")
            continue

        df = pd.read_excel(
            MASTER_FILE,
            sheet_name=sheet_name
        )

        # Remove completely empty rows/columns
        df = df.dropna(how="all")
        df = df.dropna(axis=1, how="all")

        print(f"\nSheet   : {sheet_name}")
        print(f"Rows    : {len(df):,}")
        print(f"Columns : {len(df.columns)}")
        print(f"Fields  : {list(df.columns)}")


if __name__ == "__main__":
    profile_csv_files()
    profile_master_data()