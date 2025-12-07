# Loading New Datasets - Quick Guide

## Quick Start

### Method 1: Automatic Integration (Recommended)

1. **Download your dataset** (e.g., Home Credit from Kaggle)
   ```bash
   # Place CSV in your data directory
   cp ~/Downloads/application_train.csv /usr/local/airflow/data/
   ```

2. **Run the integration script**
   ```bash
   python integrate_new_dataset.py add /usr/local/airflow/data/application_train.csv
   ```

3. **Restart Airflow**
   ```bash
   astro dev restart
   ```

4. **Trigger the DAG** in Airflow UI

That's it! The script automatically:
- ✅ Detects column mappings
- ✅ Generates staging SQL models
- ✅ Updates `raw_sources.yml`
- ✅ Updates dbt `sources.yml`

---

## Supported Datasets

The adapter automatically recognizes these datasets:

### 1. **Home Credit Default Risk** (Kaggle)
- Primary file: `application_train.csv` (307K rows, 122 columns)
- Auto-mapped columns:
  - `SK_ID_CURR` → customer_id, loan_id
  - `AMT_CREDIT` → loan_amount
  - `AMT_GOODS_PRICE` → asset_cost
  - `TARGET` → loan_default
  - `DAYS_BIRTH` → date_of_birth (converted)
  - `NAME_INCOME_TYPE` → employment_type

### 2. **Vehicle Loan Dataset** (Current)
- Already configured
- Columns: UniqueID, disbursed_amount, asset_cost, etc.

### 3. **Generic Loan Datasets**
- Will attempt auto-mapping based on common column names
- Review generated SQL before running

---

## Advanced Usage

### Analyze Dataset Without Integration
```bash
python auto_data_adapter.py application_train.csv able-balm-454718-n8 bank_dwh_demo_raw
```

This shows:
- Dataset type detected
- Number of rows/columns
- Detected column mappings
- Generated SQL (preview only)

### List Configured Datasets
```bash
python integrate_new_dataset.py list
```

### Custom Project/Dataset IDs
```bash
python integrate_new_dataset.py add application_train.csv \
  --project-id your-project-id \
  --dataset-id your_dataset_id
```

---

## File Locations

After integration, files are created/updated:

```
/usr/local/airflow/
├── config/
│   └── raw_sources.yml          # ✏️ Updated with new source
├── dbt/
│   └── models/
│       └── staging/
│           ├── sources.yml       # ✏️ Updated with new table
│           └── stg_new_data.sql  # ✨ Generated staging model
└── data/
    └── your_file.csv            # 📥 Your CSV file
```

---

## Example: Loading Home Credit Data

### Step-by-step:

1. **Download from Kaggle**
   ```bash
   # Download application_train.csv from:
   # https://www.kaggle.com/competitions/home-credit-default-risk/data
   ```

2. **Copy to data directory**
   ```bash
   cp ~/Downloads/application_train.csv /usr/local/airflow/data/
   ```

3. **Integrate the dataset**
   ```bash
   cd /usr/local/airflow/include
   python integrate_new_dataset.py add /usr/local/airflow/data/application_train.csv
   ```

   Output:
   ```
   ==================================================================
   Dataset Analysis Report
   ==================================================================
   File: application_train.csv
   Type: home_credit
   Rows: 307,511
   Columns: 122

   Detected Mappings (15):
   ------------------------------------------------------------------
     loan_id              <- SK_ID_CURR
     customer_id          <- SK_ID_CURR
     loan_amount          <- AMT_CREDIT
     asset_cost           <- AMT_GOODS_PRICE
     loan_default         <- TARGET
     employment_type      <- NAME_INCOME_TYPE
     gender               <- CODE_GENDER
     date_of_birth        <- DAYS_BIRTH
     ...
   ==================================================================

   ✓ Created backup: /usr/local/airflow/config/raw_sources.yml.backup
   ✓ Updated /usr/local/airflow/config/raw_sources.yml
   ✓ Created staging model: /usr/local/airflow/dbt/models/staging/stg_application_train.sql
   ✓ Updated /usr/local/airflow/dbt/models/staging/sources.yml

   ✓ Dataset integrated successfully!
   ```

4. **Restart Airflow**
   ```bash
   astro dev restart
   ```

5. **Run the DAG**
   - Go to Airflow UI (http://localhost:8080)
   - Find `bank_etl_dag`
   - Click "Trigger DAG"

6. **Verify in BigQuery**
   ```sql
   SELECT COUNT(*) FROM `able-balm-454718-n8.bank_dwh_demo_raw.application_train_raw`;
   SELECT * FROM `able-balm-454718-n8.bank_dwh_demo_staging.stg_application_train` LIMIT 10;
   ```

---

## Troubleshooting

### Issue: Column mapping not detected

**Solution:** Manually edit the generated staging SQL:
```bash
nano /usr/local/airflow/dbt/models/staging/stg_your_file.sql
```

Add missing columns based on your CSV structure.

### Issue: Date format incorrect

**Solution:** The script tries to auto-detect date formats. If wrong, edit the staging SQL:
```sql
-- For dd-mm-yyyy format:
SAFE.PARSE_DATE('%d-%m-%Y', cast(YourDateColumn as string)) as application_date

-- For yyyy-mm-dd format:
SAFE.PARSE_DATE('%Y-%m-%d', cast(YourDateColumn as string)) as application_date

-- For Home Credit DAYS format (days before today):
DATE_ADD(CURRENT_DATE(), INTERVAL cast(DAYS_COLUMN as int64) DAY) as date_field
```

### Issue: DAG fails with "table not found"

**Solution:** Check that:
1. CSV file is in `/usr/local/airflow/data/`
2. File path in `raw_sources.yml` is correct
3. Run `astro dev restart`

---

## Understanding the Generated Files

### Generated Staging SQL Example
```sql
{{ config(materialized='view') }}

with src as (
  select * from {{ source('raw', 'application_train_raw') }}
),

transformed as (
  select
    cast(SK_ID_CURR as string) as loan_id,
    cast(SK_ID_CURR as string) as customer_id,
    cast(AMT_CREDIT as numeric) as loan_amount,
    cast(AMT_GOODS_PRICE as numeric) as asset_cost,
    cast(TARGET as int64) as loan_default,
    date_add(current_date(), interval cast(DAYS_BIRTH as int64) day) as date_of_birth,
    cast(NAME_INCOME_TYPE as string) as employment_type
  from src
)

select * from transformed
```

This is a standard dbt staging model that:
- Reads from the raw BigQuery table
- Maps columns to your standard schema
- Casts types appropriately
- Handles date conversions

---

## Next Steps

After loading new data:

1. **Update downstream models** if needed
   - Modify `dim_customer.sql`, `f_loan_contract.sql` to use new staging model
   - Add UNION if combining with existing data

2. **Add data quality tests**
   ```yaml
   # In models/staging/schema.yml
   models:
     - name: stg_application_train
       columns:
         - name: loan_id
           tests:
             - unique
             - not_null
   ```

3. **Create dataset-specific marts**
   - Build custom fact/dimension tables for new dataset
   - Example: `f_home_credit_applications.sql`

---

## Tips

- **Start small**: Test with a sample of data first
- **Check column types**: Use `SELECT * LIMIT 10` in BigQuery to verify
- **Use backups**: The script creates `.backup` files automatically
- **Version control**: Commit generated SQL files to git
- **Incremental loads**: For large datasets, consider modifying to incremental materialization

---

## Need Help?

If the auto-mapper doesn't work for your dataset:
1. Run the analyzer to see what was detected
2. Manually create/edit the staging SQL
3. Update `raw_sources.yml` manually
4. Restart and test

The auto-mapper handles 90% of common cases, but custom datasets may need manual tweaking!