#!/usr/bin/env python3
"""
Integration script to add new datasets to the existing pipeline
Automatically updates raw_sources.yml and generates staging models
"""
import yaml
from pathlib import Path
import shutil
from auto_data_adapter import DatasetAdapter


class PipelineIntegrator:
    """Integrates new datasets into existing Airflow/dbt pipeline"""
    
    def __init__(self, airflow_home: str = "/usr/local/airflow"):
        self.airflow_home = Path(airflow_home)
        self.config_dir = self.airflow_home / "config"
        self.dbt_dir = self.airflow_home / "dbt"
        self.staging_dir = self.dbt_dir / "models" / "staging"
        self.raw_sources_file = self.config_dir / "raw_sources.yml"
        
    def add_dataset(self, 
                   csv_path: str,
                   project_id: str,
                   dataset_id: str,
                   create_backup: bool = True):
        """
        Add a new dataset to the pipeline
        
        Args:
            csv_path: Path to CSV file
            project_id: GCP project ID
            dataset_id: BigQuery dataset ID
            create_backup: Whether to backup existing configs
        """
        csv_path = Path(csv_path)
        
        print(f"\n{'='*70}")
        print(f"Adding new dataset: {csv_path.name}")
        print(f"{'='*70}\n")
        
        # 1. Analyze dataset
        adapter = DatasetAdapter(str(csv_path))
        adapter.analyze_and_report()
        
        # 2. Generate names
        table_name = csv_path.stem.lower().replace('-', '_').replace(' ', '_')
        source_name = f"{table_name}_raw"
        staging_model_name = f"stg_{table_name}"
        
        # 3. Backup existing config if requested
        if create_backup and self.raw_sources_file.exists():
            backup_file = self.raw_sources_file.with_suffix('.yml.backup')
            shutil.copy(self.raw_sources_file, backup_file)
            print(f"✓ Created backup: {backup_file}")
        
        # 4. Update raw_sources.yml
        self._update_raw_sources_config(
            source_name=source_name,
            csv_path=csv_path,
            project_id=project_id,
            dataset_id=dataset_id
        )
        
        # 5. Generate staging model
        staging_sql = adapter.generate_staging_sql(
            model_name=staging_model_name,
            source_name=source_name
        )
        
        # 6. Write staging model
        staging_file = self.staging_dir / f"{staging_model_name}.sql"
        with open(staging_file, 'w') as f:
            f.write(staging_sql)
        print(f"✓ Created staging model: {staging_file}")
        
        # 7. Update dbt sources.yml
        self._update_dbt_sources(source_name, table_name)
        
        print(f"\n{'='*70}")
        print(f"✓ Dataset integrated successfully!")
        print(f"{'='*70}")
        print(f"\nNext steps:")
        print(f"1. Copy {csv_path.name} to {self.airflow_home / 'data' / csv_path.name}")
        print(f"2. Run: astro dev restart")
        print(f"3. Trigger the DAG in Airflow UI")
        print(f"4. Check staging model: {staging_model_name}")
        
    def _update_raw_sources_config(self, 
                                   source_name: str,
                                   csv_path: Path,
                                   project_id: str,
                                   dataset_id: str):
        """Update raw_sources.yml with new source"""
        
        # Load existing config
        if self.raw_sources_file.exists():
            with open(self.raw_sources_file) as f:
                config = yaml.safe_load(f) or {}
        else:
            config = {}
        
        if 'raw_sources' not in config:
            config['raw_sources'] = []
        
        # Check if source already exists
        existing_names = [s['name'] for s in config['raw_sources']]
        if source_name in existing_names:
            print(f"⚠ Source '{source_name}' already exists in config, skipping...")
            return
        
        # Add new source
        new_source = {
            'name': source_name,
            'project_id': project_id,
            'dataset_id': dataset_id,
            'table_id': source_name,
            'csv_path': f"/usr/local/airflow/data/{csv_path.name}"
        }
        
        config['raw_sources'].append(new_source)
        
        # Write updated config
        with open(self.raw_sources_file, 'w') as f:
            yaml.dump(config, f, default_flow_style=False, sort_keys=False)
        
        print(f"✓ Updated {self.raw_sources_file}")
    
    def _update_dbt_sources(self, source_name: str, table_name: str):
        """Update dbt sources.yml"""
        sources_file = self.dbt_dir / "models" / "staging" / "sources.yml"
        
        if not sources_file.exists():
            # Create new sources.yml
            sources_config = {
                'version': 2,
                'sources': [
                    {
                        'name': 'raw',
                        'database': 'vivid-layout-453307-p4',
                        'schema': 'ryoji_raw_demo',
                        'tables': [
                            {'name': source_name}
                        ]
                    }
                ]
            }
        else:
            # Load and update existing
            with open(sources_file) as f:
                sources_config = yaml.safe_load(f)
            
            # Find raw source
            raw_source = None
            for source in sources_config['sources']:
                if source['name'] == 'raw':
                    raw_source = source
                    break
            
            if raw_source:
                # Check if table already exists
                existing_tables = [t['name'] for t in raw_source.get('tables', [])]
                if source_name not in existing_tables:
                    if 'tables' not in raw_source:
                        raw_source['tables'] = []
                    raw_source['tables'].append({'name': source_name})
        
        # Write updated sources
        with open(sources_file, 'w') as f:
            yaml.dump(sources_config, f, default_flow_style=False, sort_keys=False)
        
        print(f"✓ Updated {sources_file}")
    
    def list_datasets(self):
        """List all configured datasets"""
        if not self.raw_sources_file.exists():
            print("No datasets configured yet.")
            return
        
        with open(self.raw_sources_file) as f:
            config = yaml.safe_load(f) or {}
        
        sources = config.get('raw_sources', [])
        
        print(f"\n{'='*70}")
        print(f"Configured Datasets ({len(sources)})")
        print(f"{'='*70}")
        
        for i, source in enumerate(sources, 1):
            print(f"\n{i}. {source['name']}")
            print(f"   Project: {source['project_id']}")
            print(f"   Dataset: {source['dataset_id']}")
            print(f"   CSV: {source['csv_path']}")


def main():
    import argparse
    
    parser = argparse.ArgumentParser(
        description='Integrate new datasets into the Bank DWH pipeline'
    )
    
    subparsers = parser.add_subparsers(dest='command', help='Commands')
    
    # Add dataset command
    add_parser = subparsers.add_parser('add', help='Add a new dataset')
    add_parser.add_argument('csv_path', help='Path to CSV file')
    add_parser.add_argument('--project-id', default='vivid-layout-453307-p4',
                           help='GCP project ID')
    add_parser.add_argument('--dataset-id', default='ryoji_raw_demo',
                           help='BigQuery dataset ID')
    add_parser.add_argument('--no-backup', action='store_true',
                           help='Skip creating backup of existing config')
    
    # List datasets command
    list_parser = subparsers.add_parser('list', help='List configured datasets')
    
    args = parser.parse_args()
    
    integrator = PipelineIntegrator()
    
    if args.command == 'add':
        integrator.add_dataset(
            csv_path=args.csv_path,
            project_id=args.project_id,
            dataset_id=args.dataset_id,
            create_backup=not args.no_backup
        )
    elif args.command == 'list':
        integrator.list_datasets()
    else:
        parser.print_help()


if __name__ == "__main__":
    main()