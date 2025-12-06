# include/load_raw_to_bq.py
import os
from pathlib import Path
import csv
import tempfile

from google.cloud import bigquery


def _sanitize_header(src_path: Path) -> Path:
    """Create a temp CSV with the same rows but header '.' replaced by '_'."""
    tmp_fd, tmp_name = tempfile.mkstemp(suffix="_clean.csv")
    os.close(tmp_fd)  # close low-level fd, we'll reopen with csv

    tmp_path = Path(tmp_name)

    with src_path.open("r", newline="", encoding="utf-8") as f_in, \
         tmp_path.open("w", newline="", encoding="utf-8") as f_out:
        reader = csv.reader(f_in)
        writer = csv.writer(f_out)

        header = next(reader)
        new_header = [col.replace(".", "_") for col in header]
        writer.writerow(new_header)

        for row in reader:
            writer.writerow(row)

    print(f"[load_raw_to_bq] Cleaned header for {src_path} -> {tmp_path}")
    return tmp_path


def load_csv_to_bq(
    project_id: str,
    dataset_id: str,
    table_id: str,
    csv_path: str,
    sanitize_header: bool = False,
) -> None:
    """
    Load a local CSV into BigQuery.

    csv_path can be:
      - absolute (e.g. /usr/local/airflow/data/customers.csv), or
      - relative to AIRFLOW_HOME (e.g. data/customers.csv)

    If sanitize_header=True, we replace '.' with '_' in the header row
    before loading (useful for Kaggle / weird column names).
    """

    airflow_home = Path(os.environ.get("AIRFLOW_HOME", "/usr/local/airflow"))

    csv_path = Path(csv_path)
    if not csv_path.is_absolute():
        csv_path = airflow_home / csv_path

    if not csv_path.exists():
        raise FileNotFoundError(f"CSV file not found at: {csv_path}")

    # Optional: header sanitization step
    if sanitize_header:
        csv_path = _sanitize_header(csv_path)

    client = bigquery.Client(project=project_id)
    full_table_id = f"{project_id}.{dataset_id}.{table_id}"

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.CSV,
        skip_leading_rows=1,
        autodetect=True,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
    )

    with open(csv_path, "rb") as f:
        load_job = client.load_table_from_file(f, full_table_id, job_config=job_config)

    load_job.result()
    print(f"Loaded {csv_path} into {full_table_id}")

