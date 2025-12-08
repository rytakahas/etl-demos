#!/usr/bin/env python3
"""
Integration script to add new datasets to the existing pipeline.

What it does:
  1) Analyze a new CSV with DatasetAdapter (auto_data_adapter.py)
  2) Append a new entry to config/raw_sources.yml  (for Airflow → Bronze)
  3) Generate a staging model: dbt/models/staging/stg_<dataset>.sql  (Silver)
  4) Update dbt/models/staging/staging.yml to register the new raw table
     and add a simple models: block for the new staging model.

NOTE: This version no longer creates or updates 'sources.yml'.
      It assumes you are using only 'staging.yml' for dbt sources.
"""

import yaml
from pathlib import Path
import shutil
from auto_data_adapter import DatasetAdapter


class PipelineIntegrator:
    """Integrates new datasets into existing Airflow/dbt pipeline."""
    
    def __init__(self, airflow_home: str = "/usr/local/airflow"):
        self.airflow_home = Path(airflow_home)
        self.config_dir = self.airflow_home / "config"
        self.dbt_dir = self.airflow_home / "dbt"
        self.staging_dir = self.dbt_dir / "models" / "staging"
        self.raw_sources_file = self.config_dir / "raw_sources.yml"
        # single canonical dbt config file
        self.staging_yml_file = self.staging_dir / "staging.yml"
        
    def add_dataset(
        self, 
        csv_path: str,
        project_id: str,
        dataset_id: str,
        create_backup: bool = True
    ):
        """
        Add a new dataset to the pipeline.

        Args:
            csv_path: Path to CSV file (inside container, e.g. data/application_train.csv)
            project_id: GCP project ID of the RAW dataset (e.g. vivid-layout-453307-p4)
            dataset_id: BigQuery RAW dataset ID (e.g. ryoji_raw_demos)
            create_backup: Whether to backup existing configs
        """
        csv_path = Path(csv_path)
        
        print(f"\n{'='*70}")
        print(f"Adding new dataset: {csv_path.name}")
        print(f"{'='*70}\n")
        
        # 1. Analyze dataset (auto-detect mappings)
        adapter = DatasetAdapter(str(self.airflow_home / csv_path))
        adapter.analyze_and_report()
        
        # 2. Generate canonical names
        table_name = csv_path.stem.lower().replace("-", "_").replace(" ", "_")
        source_name = f"{table_name}_raw"          # e.g. application_train_raw
        staging_model_name = f"stg_{table_name}"   # e.g. stg_application_train
        
        # 3. Backup existing config if requested
        if create_backup and self.raw_sources_file.exists():
            backup_file = self.raw_sources_file.with_suffix(".yml.backup")
            shutil.copy(self.raw_sources_file, backup_file)
            print(f"✓ Created backup: {backup_file}")
        
        # 4. Update raw_sources.yml (for Airflow ingestion → Bronze)
        self._update_raw_sources_config(
            source_name=source_name,
            csv_path=csv_path,
            project_id=project_id,
            dataset_id=dataset_id,
        )
        
        # 5. Generate staging model SQL (Silver)
        staging_sql = adapter.generate_staging_sql(
            model_name=staging_model_name,
            source_name=source_name,
        )
        
        # 6. Write staging model file
        self.staging_dir.mkdir(parents=True, exist_ok=True)
        staging_file = self.staging_dir / f"{staging_model_name}.sql"
        with open(staging_file, "w") as f:
            f.write(staging_sql)
        print(f"✓ Created staging model: {staging_file}")
        
        # 7. Update dbt/models/staging/staging.yml (single source of truth)
        self._update_staging_yml(
            source_name=source_name,
            project_id=project_id,
            dataset_id=dataset_id,
            staging_model_name=staging_model_name,
        )
        
        print(f"\n{'='*70}")
        print(f"✓ Dataset integrated successfully!")
        print(f"{'='*70}")
        print("\nNext steps:")
        print(f"1. Ensure the CSV is mounted at {self.airflow_home / csv_path}")
        print("2. Run: astro dev restart")
        print("3. Trigger the DAG in Airflow UI (or dbt run from /usr/local/airflow/dbt)")
        print(f"4. Check staging model in BigQuery: R   ryoji_wh_demos.{staging_model_name}")
    
    # -------------------------------------------------------------------------
    #  Internal helpers
    # -------------------------------------------------------------------------
    
    def _update_raw_sources_config(
        self, 
        source_name: str,
        csv_path: Path,
        project_id: str,
        dataset_id: str,
    ):
        """Update config/raw_sources.yml with new raw source."""
        
        # Load existing config
        if self.raw_sources_file.exists():
            with open(self.raw_sources_file) as f:
                config = yaml.safe_load(f) or {}
        else:
            config = {}
        
        if "raw_sources" not in config:
            config["raw_sources"] = []
        
        # Check if source already exists
        existing_names = [s.get("name") for s in config["raw_sources"]]
        if source_name in existing_names:
            print(f"⚠ Source '{source_name}' already exists in raw_sources.yml, skipping...")
            return
        
        # Add new source
        new_source = {
            "name": source_name,
            "project_id": project_id,
            "dataset_id": dataset_id,
            "table_id": source_name,
            # keep absolute path since this runs inside Airflow container
            "csv_path": f"/usr/local/airflow/data/{csv_path.name}",
        }
        
        config["raw_sources"].append(new_source)
        
        # Write updated config
        self.config_dir.mkdir(parents=True, exist_ok=True)
        with open(self.raw_sources_file, "w") as f:
            yaml.dump(config, f, default_flow_style=False, sort_keys=False)
        
        print(f"✓ Updated {self.raw_sources_file}")
    
    def _update_staging_yml(
        self, 
        source_name: str,
        project_id: str,
        dataset_id: str,
        staging_model_name: str,
    ):
        """
        Update dbt/models/staging/staging.yml:
          - Ensure 'raw' source exists with correct database/schema
          - Add the new table under raw.tables[]
          - Add a basic models: entry for the staging model (optional)
        """
        # Load existing staging.yml or create a new skeleton
        if self.staging_yml_file.exists():
            with open(self.staging_yml_file) as f:
                config = yaml.safe_load(f) or {}
        else:
            config = {
                "version": 2,
                "sources": [],
                "models": [],
            }
        
        # Ensure 'version' exists
        if "version" not in config:
            config["version"] = 2
        
        # Ensure 'sources' and 'models' keys
        if "sources" not in config:
            config["sources"] = []
        if "models" not in config:
            config["models"] = []
        
        # --- Update / create the 'raw' source block ---
        raw_source = None
        for src in config["sources"]:
            if src.get("name") == "raw":
                raw_source = src
                break
        
        if raw_source is None:
            raw_source = {
                "name": "raw",
                "database": project_id,
                "schema": dataset_id,
                "tables": [],
            }
            config["sources"].append(raw_source)
        else:
            # Do NOT overwrite manually-tuned database/schema if they exist.
            # Only fill defaults if missing.
            raw_source.setdefault("database", project_id)
            raw_source.setdefault("schema", dataset_id)
            raw_source.setdefault("tables", [])
        
        # Add new table to raw.tables if not already there
        existing_table_names = [t.get("name") for t in raw_source.get("tables", [])]
        if source_name not in existing_table_names:
            raw_source["tables"].append({"name": source_name})
            print(f"✓ Registered raw source table in staging.yml: {source_name}")
        else:
            print(f"⚠ Table '{source_name}' already in staging.yml sources, skipping...")
        
        # --- Add basic model tests for the new staging model (optional, simple) ---
        existing_model_names = [m.get("name") for m in config.get("models", [])]
        if staging_model_name not in existing_model_names:
            # Very lightweight tests; you can edit by hand later
            new_model = {
                "name": staging_model_name,
                "columns": [
                    {"name": "loan_id", "tests": ["not_null"]},
                    {"name": "customer_id", "tests": ["not_null"]},
                ],
            }
            config["models"].append(new_model)
            print(f"✓ Added basic tests for model: {staging_model_name} in staging.yml")
        else:
            print(f"⚠ Model '{staging_model_name}' already has config in staging.yml, skipping...")
        
        # Write back staging.yml
        self.staging_dir.mkdir(parents=True, exist_ok=True)
        with open(self.staging_yml_file, "w") as f:
            yaml.dump(config, f, default_flow_style=False, sort_keys=False)
        
        print(f"✓ Updated {self.staging_yml_file}")
    
    # -------------------------------------------------------------------------
    #  Utility: list configured datasets
    # -------------------------------------------------------------------------
    
    def list_datasets(self):
        """List all configured raw datasets (from raw_sources.yml)."""
        if not self.raw_sources_file.exists():
            print("No datasets configured yet (raw_sources.yml missing).")
            return
        
        with open(self.raw_sources_file) as f:
            config = yaml.safe_load(f) or {}
        
        sources = config.get("raw_sources", [])
        
        print(f"\n{'='*70}")
        print(f"Configured Raw Datasets ({len(sources)})")
        print(f"{'='*70}")
        
        for i, source in enumerate(sources, 1):
            print(f"\n{i}. {source.get('name')}")
            print(f"   Project: {source.get('project_id')}")
            print(f"   Dataset: {source.get('dataset_id')}")
            print(f"   Table:   {source.get('table_id')}")
            print(f"   CSV:     {source.get('csv_path')}")


def main():
    import argparse
    
    parser = argparse.ArgumentParser(
        description="Integrate new datasets into the Bank DWH pipeline"
    )
    
    subparsers = parser.add_subparsers(dest="command", help="Commands")
    
    # Add dataset command
    add_parser = subparsers.add_parser("add", help="Add a new dataset")
    add_parser.add_argument("csv_path", help="Path to CSV file (relative to AIRFLOW_HOME or absolute)")
    add_parser.add_argument(
        "--project-id",
        default="vivid-layout-453307-p4",
        help="GCP project ID for RAW dataset (default: vivid-layout-453307-p4)",
    )
    add_parser.add_argument(
        "--dataset-id",
        default="ryoji_raw_demos",
        help="BigQuery RAW dataset ID (default: ryoji_raw_demos)",
    )
    add_parser.add_argument(
        "--no-backup",
        action="store_true",
        help="Skip creating backup of existing raw_sources.yml",
    )
    
    # List datasets command
    list_parser = subparsers.add_parser("list", help="List configured raw datasets")
    
    args = parser.parse_args()
    
    integrator = PipelineIntegrator()
    
    if args.command == "add":
        integrator.add_dataset(
            csv_path=args.csv_path,
            project_id=args.project_id,
            dataset_id=args.dataset_id,
            create_backup=not args.no_backup,
        )
    elif args.command == "list":
        integrator.list_datasets()
    else:
        parser.print_help()


if __name__ == "__main__":
    main()

