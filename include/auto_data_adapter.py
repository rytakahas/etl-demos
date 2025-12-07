"""
Automatic Data Adapter for Bank DWH Demo
Detects dataset structure and generates appropriate staging models automatically
"""
import pandas as pd
import yaml
from pathlib import Path
from typing import Dict, List, Tuple
import re


class DatasetAdapter:
    """Automatically adapts different loan datasets to the DWH schema"""
    
    # Column mapping patterns for different datasets
    COLUMN_MAPPINGS = {
        # Loan/Application ID
        'loan_id': ['uniqueid', 'sk_id_curr', 'loan_id', 'application_id', 'contract_id'],
        'customer_id': ['uniqueid', 'sk_id_curr', 'customer_id', 'client_id'],
        
        # Amounts
        'loan_amount': ['disbursed_amount', 'amt_credit', 'loan_amount', 'credit_amount'],
        'asset_cost': ['asset_cost', 'amt_goods_price', 'goods_price'],
        
        # Dates
        'application_date': ['disbursaldate', 'days_decision', 'application_date', 'disbursal_date'],
        'date_of_birth': ['date_of_birth', 'days_birth'],
        
        # Target
        'loan_default': ['loan_default', 'target', 'default_flag'],
        
        # Customer attributes
        'employment_type': ['employment_type', 'name_income_type', 'occupation_type'],
        'gender': ['code_gender', 'gender'],
        
        # Location
        'state_id': ['state_id', 'region_rating_client'],
        'branch_id': ['branch_id', 'dealer_id'],
        'pincode_id': ['current_pincode_id', 'region_population_relative'],
        
        # Product
        'product_id': ['manufacturer_id', 'product_id', 'name_contract_type'],
        
        # Credit bureau
        'credit_score': ['perform_cns_score', 'ext_source_1', 'ext_source_2', 'ext_source_3'],
        'ltv_ratio': ['ltv', 'amt_credit_sum_debt'],
    }
    
    def __init__(self, csv_path: str):
        self.csv_path = Path(csv_path)
        self.df = None
        self.detected_mappings = {}
        self.dataset_type = None
        
    def detect_dataset_type(self) -> str:
        """Detect which dataset this is based on column patterns"""
        print(f"Analyzing {self.csv_path.name}...")
        
        # Read first few rows to detect structure
        self.df = pd.read_csv(self.csv_path, nrows=5)
        columns_lower = [col.lower() for col in self.df.columns]
        
        if 'sk_id_curr' in columns_lower:
            return 'home_credit'
        elif 'uniqueid' in columns_lower and 'disbursaldate' in columns_lower:
            return 'vehicle_loan'
        else:
            return 'generic'
    
    def map_columns(self) -> Dict[str, str]:
        """Map dataset columns to standard schema"""
        mappings = {}
        columns_lower = {col.lower(): col for col in self.df.columns}
        
        for standard_col, possible_names in self.COLUMN_MAPPINGS.items():
            for possible_name in possible_names:
                if possible_name in columns_lower:
                    mappings[standard_col] = columns_lower[possible_name]
                    break
        
        self.detected_mappings = mappings
        return mappings
    
    def generate_staging_sql(self, model_name: str, source_name: str) -> str:
        """Generate dbt staging model SQL"""
        
        sql_parts = [
            "{{ config(materialized='view') }}",
            "",
            "with src as (",
            f"  select * from {{{{ source('raw', '{source_name}') }}}}",
            "),",
            "",
            "transformed as (",
            "  select"
        ]
        
        # Generate column mappings
        select_cols = []
        
        # Loan/Customer ID
        if 'loan_id' in self.detected_mappings:
            select_cols.append(
                f"    cast({self.detected_mappings['loan_id']} as string) as loan_id"
            )
        
        if 'customer_id' in self.detected_mappings:
            select_cols.append(
                f"    cast({self.detected_mappings['customer_id']} as string) as customer_id"
            )
        
        # Handle dates with appropriate parsing
        if 'application_date' in self.detected_mappings:
            date_col = self.detected_mappings['application_date']
            if 'days' in date_col.lower():
                # Home Credit style (days from today)
                select_cols.append(
                    f"    date_add(current_date(), interval cast({date_col} as int64) day) as application_date"
                )
            else:
                # Standard date format
                select_cols.append(
                    f"    safe.parse_date('%d-%m-%y', cast({date_col} as string)) as application_date"
                )
        
        if 'date_of_birth' in self.detected_mappings:
            dob_col = self.detected_mappings['date_of_birth']
            if 'days' in dob_col.lower():
                select_cols.append(
                    f"    date_add(current_date(), interval cast({dob_col} as int64) day) as date_of_birth"
                )
            else:
                select_cols.append(
                    f"    safe.parse_date('%d-%m-%y', cast({dob_col} as string)) as date_of_birth"
                )
        
        # Amounts
        for col in ['loan_amount', 'asset_cost', 'ltv_ratio']:
            if col in self.detected_mappings:
                select_cols.append(
                    f"    cast({self.detected_mappings[col]} as numeric) as {col}"
                )
        
        # String columns
        for col in ['employment_type', 'gender', 'state_id', 'branch_id', 'pincode_id', 'product_id']:
            if col in self.detected_mappings:
                select_cols.append(
                    f"    cast({self.detected_mappings[col]} as string) as {col}"
                )
        
        # Target
        if 'loan_default' in self.detected_mappings:
            select_cols.append(
                f"    cast({self.detected_mappings['loan_default']} as int64) as loan_default"
            )
        
        # Credit score
        if 'credit_score' in self.detected_mappings:
            select_cols.append(
                f"    cast({self.detected_mappings['credit_score']} as int64) as credit_score"
            )
        
        # Join all select columns
        sql_parts.append(",\n".join(select_cols))
        sql_parts.extend([
            "  from src",
            ")",
            "",
            "select * from transformed"
        ])
        
        return "\n".join(sql_parts)
    
    def generate_raw_sources_config(self, 
                                   project_id: str,
                                   dataset_id: str,
                                   table_id: str) -> Dict:
        """Generate configuration for raw_sources.yml"""
        return {
            'name': table_id,
            'project_id': project_id,
            'dataset_id': dataset_id,
            'table_id': table_id,
            'csv_path': str(self.csv_path.absolute())
        }
    
    def analyze_and_report(self):
        """Analyze dataset and print report"""
        self.dataset_type = self.detect_dataset_type()
        self.map_columns()
        
        print(f"\n{'='*60}")
        print(f"Dataset Analysis Report")
        print(f"{'='*60}")
        print(f"File: {self.csv_path.name}")
        print(f"Type: {self.dataset_type}")
        print(f"Rows: {len(pd.read_csv(self.csv_path)):,}")
        print(f"Columns: {len(self.df.columns)}")
        print(f"\nDetected Mappings ({len(self.detected_mappings)}):")
        print(f"{'-'*60}")
        for standard, actual in self.detected_mappings.items():
            print(f"  {standard:20} <- {actual}")
        print(f"{'='*60}\n")


def process_dataset(csv_path: str,
                   project_id: str,
                   dataset_id: str,
                   output_dir: str = None):
    """
    Process a dataset and generate all necessary configurations
    
    Args:
        csv_path: Path to CSV file
        project_id: GCP project ID
        dataset_id: BigQuery dataset ID
        output_dir: Optional output directory for generated files
    """
    adapter = DatasetAdapter(csv_path)
    adapter.analyze_and_report()
    
    # Generate table name from filename
    table_id = Path(csv_path).stem.lower().replace('-', '_')
    
    # Generate staging model
    source_name = f"{table_id}_raw"
    staging_sql = adapter.generate_staging_sql(
        model_name=f"stg_{table_id}",
        source_name=source_name
    )
    
    # Generate raw sources config
    raw_config = adapter.generate_raw_sources_config(
        project_id=project_id,
        dataset_id=dataset_id,
        table_id=source_name
    )
    
    # Output results
    if output_dir:
        output_path = Path(output_dir)
        output_path.mkdir(exist_ok=True)
        
        # Write staging SQL
        sql_file = output_path / f"stg_{table_id}.sql"
        with open(sql_file, 'w') as f:
            f.write(staging_sql)
        print(f"✓ Generated staging model: {sql_file}")
        
        # Write config
        config_file = output_path / f"{table_id}_config.yml"
        with open(config_file, 'w') as f:
            yaml.dump({'raw_sources': [raw_config]}, f, default_flow_style=False)
        print(f"✓ Generated config: {config_file}")
    else:
        print("\nGenerated Staging SQL:")
        print("=" * 60)
        print(staging_sql)
        print("\nGenerated Config:")
        print("=" * 60)
        print(yaml.dump({'raw_sources': [raw_config]}, default_flow_style=False))


if __name__ == "__main__":
    import sys
    
    if len(sys.argv) < 4:
        print("Usage: python auto_data_adapter.py <csv_path> <project_id> <dataset_id> [output_dir]")
        print("\nExample:")
        print("  python auto_data_adapter.py application_train.csv able-balm-454718-n8 bank_dwh_demo_raw ./generated")
        sys.exit(1)
    
    csv_path = sys.argv[1]
    project_id = sys.argv[2]
    dataset_id = sys.argv[3]
    output_dir = sys.argv[4] if len(sys.argv) > 4 else None
    
    process_dataset(csv_path, project_id, dataset_id, output_dir)