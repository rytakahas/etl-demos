# scripts/clean_vehicle_loans.py
import csv
from pathlib import Path


def clean_header(src: str, dst: str) -> None:
    src_path = Path(src)
    dst_path = Path(dst)

    with src_path.open("r", newline="", encoding="utf-8") as f_in, \
         dst_path.open("w", newline="", encoding="utf-8") as f_out:
        reader = csv.reader(f_in)
        writer = csv.writer(f_out)

        header = next(reader)
        # Replace '.' with '_' in all column names
        new_header = [col.replace(".", "_") for col in header]

        print("Old header:", header)
        print("New header:", new_header)

        writer.writerow(new_header)
        for row in reader:
            writer.writerow(row)

    print(f"Wrote cleaned CSV to: {dst_path}")


if __name__ == "__main__":
    clean_header(
        "data/vehicle_loans_train.csv",
        "data/vehicle_loans_train_clean.csv",
    )

