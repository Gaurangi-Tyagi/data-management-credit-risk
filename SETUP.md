# Setup & Reproduction Guide

## Prerequisites

- Databricks Account (Free Edition works)
- Python 3.8+
- Git (for version control)
- SQL knowledge (basic)
- Access to public datasets (Kaggle, World Bank API)

---

## Step 1: Prepare Data Files

### Download Home Credit CSV
1. Visit: https://www.kaggle.com/datasets/megancrenshaw/home-credit-default-risk
2. Download: `application_train.csv`
3. Size: ~307,511 rows

### Get World Bank JSON Data
1. Run Python script to fetch from API:

```python
import requests
import json

countries = ['DEU', 'USA', 'GBR', 'FRA', 'IND']
data = []

for country in countries:
    url = f"https://api.worldbank.org/v2/country/{country}/indicator/NY.GDP.MKTP.CD?format=json"
    response = requests.get(url)
    json_data = response.json()
    
    if len(json_data) > 1:
        for record in json_data[1]:
            if record and record.get('value'):
                data.append({
                    'country': record['countryiso3code'],
                    'country_name': record['country']['value'],
                    'year': record['date'],
                    'gdp': float(record['value'])
                })

# Save to JSON
with open('world_bank_data.json', 'w') as f:
    json.dump(data, f, indent=2)

print(f"Downloaded {len(data)} records")
```

---

## Step 2: Upload Data to Databricks

1. **Create Volume**
   ```sql
   CREATE VOLUME IF NOT EXISTS workspace.default.data_management;
   ```

2. **Upload Files**
   - Go to Workspace → Volumes
   - Navigate to `/Volumes/workspace/default/data_management/`
   - Upload `application_train.csv`
   - Upload `world_bank_data.json`

3. **Verify Upload**
   ```python
   dbutils.fs.ls("/Volumes/workspace/default/data_management/")
   ```

---

## Step 3: Create Database

```sql
CREATE DATABASE IF NOT EXISTS dm2_credit_risk
COMMENT 'Data Management 2 - Credit Risk Analysis';
```

---

## Step 4: Run Notebooks in Order

### 4.1 Bronze Layer Notebook

**File:** `notebooks/Bronze_Layer.py`

```python
from pyspark.sql import functions as F

# Create database
spark.sql("""
    CREATE DATABASE IF NOT EXISTS dm2_credit_risk
    COMMENT 'Data Management 2 - Credit Risk Final Project'
""")

# Load CSV
df_bronze_csv = spark.read.csv(
    "/Volumes/workspace/default/data_management/application_train.csv",
    header=True, 
    inferSchema=True
)

# Load JSON (with multiLine option!)
df_bronze_json = spark.read.option("multiLine", "true").json(
    "/Volumes/workspace/default/data_management/world_bank_data.json"
)

# Save as Delta tables
df_bronze_csv.write.mode("overwrite").saveAsTable("dm2_credit_risk.bronze_loan_apps")
df_bronze_json.write.mode("overwrite").saveAsTable("dm2_credit_risk.bronze_world_bank")

# Verify
spark.sql("""
    SELECT table_name FROM information_schema.tables
    WHERE table_schema = 'dm2_credit_risk'
""").display()
```

**Expected Output:** 2 bronze tables created

---

### 4.2 Silver Layer Notebook

**File:** `notebooks/Silver_Layer.py`

```python
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, DoubleType

# Read bronze tables
df_bronze_csv = spark.table("dm2_credit_risk.bronze_loan_apps")
df_bronze_json = spark.table("dm2_credit_risk.bronze_world_bank")

# Clean loan applications
df_silver_csv = (df_bronze_csv
    .filter(
        (F.col("SK_ID_CURR").isNotNull()) &
        (F.col("TARGET").isNotNull()) &
        (F.col("AMT_CREDIT") > 0) &
        (F.col("AMT_INCOME_TOTAL") > 0)
    )
    .select(
        F.col("SK_ID_CURR"), 
        F.col("TARGET"),
        F.coalesce(F.col("AMT_CREDIT"), F.lit(0)).alias("AMT_CREDIT"),
        F.coalesce(F.col("AMT_INCOME_TOTAL"), F.lit(0)).alias("AMT_INCOME_TOTAL"),
        F.coalesce(F.col("AMT_ANNUITY"), F.lit(0)).alias("AMT_ANNUITY"),
        F.coalesce(F.col("DAYS_BIRTH"), F.lit(0)).alias("DAYS_BIRTH"),
        F.coalesce(F.col("DAYS_EMPLOYED"), F.lit(0)).alias("DAYS_EMPLOYED"),
        F.coalesce(F.col("CNT_CHILDREN"), F.lit(0)).alias("CNT_CHILDREN"),
        F.coalesce(F.col("NAME_FAMILY_STATUS"), F.lit("Unknown")).alias("family_status"),
        F.coalesce(F.col("NAME_EDUCATION_TYPE"), F.lit("Unknown")).alias("education_type"),
        F.coalesce(F.col("OCCUPATION_TYPE"), F.lit("Unknown")).alias("occupation_type"),
        F.coalesce(F.col("NAME_CONTRACT_TYPE"), F.lit("Unknown")).alias("contract_type"),
        F.when(F.col("AMT_INCOME_TOTAL") > 0,
               F.round(F.col("AMT_CREDIT") / F.col("AMT_INCOME_TOTAL"), 2)
        ).otherwise(0).alias("credit_to_income_ratio"),
        F.current_timestamp().alias("processing_date")
    )
)

# Clean world bank data
df_silver_json = (df_bronze_json
    .filter(
        (F.col("gdp").isNotNull()) &
        (F.col("year").isNotNull()) &
        (F.col("country").isNotNull())
    )
    .select(
        F.upper(F.col("country")).alias("country_code"),
        F.col("country_name"),
        F.col("year").cast(IntegerType()).alias("year"),
        F.round(F.col("gdp").cast(DoubleType()), 2).alias("gdp_usd"),
        F.current_timestamp().alias("processing_date")
    )
    .filter(F.col("year") >= 2018)
)

# Save silver tables
df_silver_csv.write.mode("overwrite").saveAsTable("dm2_credit_risk.silver_loan_apps")
df_silver_json.write.mode("overwrite").saveAsTable("dm2_credit_risk.silver_world_bank")

print("✅ Silver layer created successfully!")
```

**Expected Output:** 2 silver tables with cleaned data

---

### 4.3 Gold Layer Notebook

**Cell 1 (Python):**
```python
df_silver_csv = spark.table("dm2_credit_risk.silver_loan_apps")
df_silver_json = spark.table("dm2_credit_risk.silver_world_bank")

print(f"silver_loan_apps: {df_silver_csv.count():,} rows")
print(f"silver_world_bank: {df_silver_json.count():,} rows")
```

**Cell 2 (SQL - MUST BE IN SEPARATE CELL):**
```sql
%sql
CREATE OR REPLACE TABLE dm2_credit_risk.gold_credit_risk_metrics AS
SELECT
    CASE
        WHEN credit_to_income_ratio > 3 THEN 'High Risk'
        WHEN credit_to_income_ratio > 1.5 THEN 'Medium Risk'
        ELSE 'Low Risk'
    END as risk_segment,
    family_status,
    education_type,
    COUNT(SK_ID_CURR) as total_customers,
    SUM(CASE WHEN TARGET = 1 THEN 1 ELSE 0 END) as defaulted_customers,
    ROUND(SUM(CASE WHEN TARGET = 1 THEN 1 ELSE 0 END) / COUNT(SK_ID_CURR) * 100, 2) as default_rate_percent,
    ROUND(AVG(AMT_CREDIT), 2) as avg_credit_amount,
    ROUND(AVG(AMT_INCOME_TOTAL), 2) as avg_income,
    ROUND(AVG(credit_to_income_ratio), 2) as avg_credit_to_income_ratio,
    COUNT(DISTINCT SK_ID_CURR) as unique_customers
FROM dm2_credit_risk.silver_loan_apps
GROUP BY risk_segment, family_status, education_type
ORDER BY default_rate_percent DESC;

SELECT * FROM dm2_credit_risk.gold_credit_risk_metrics LIMIT 10;
```

**Expected Output:** Gold table with 77 rows of business metrics

---

## Step 5: Create Dashboard

### Option A: Manual Creation
1. Go to SQL Editor
2. Run each query from `sql/queries/` folder
3. Create visualizations
4. Add to dashboard

### Option B: Import JSON (Faster)
1. Download `Credit_Risk_Dashboard.json`
2. Go to Databricks Dashboards
3. Click "Import" → Upload JSON
4. Dashboard auto-created!

---

## Step 6: Schedule Job

1. Go to **Jobs & Pipelines**
2. Click **Create Job**
3. Name: `dm2_credit_risk_batch_job`
4. Task: `Gold_Layer` notebook
5. Schedule: Daily 9:00 AM
6. Click **Create**

---

## Step 7: Verify Everything Works

```sql
-- Check all tables exist
SELECT * FROM dm2_credit_risk.gold_credit_risk_metrics LIMIT 5;

-- Check row counts
SELECT COUNT(*) as metrics_rows FROM dm2_credit_risk.gold_credit_risk_metrics;

-- Check default rate
SELECT 
    risk_segment,
    ROUND(AVG(default_rate_percent), 2) as avg_default_rate
FROM dm2_credit_risk.gold_credit_risk_metrics
GROUP BY risk_segment;
```

---

## Common Issues & Fixes

| Issue | Solution |
|-------|----------|
| `multiLine` error in JSON | Add `option("multiLine", "true")` to JSON read |
| `DAYS_EMPLOYMENT not found` | Use `DAYS_EMPLOYED` instead |
| `%sql` syntax error | Put SQL in separate cell from Python |
| Volume not found | Create volume: `CREATE VOLUME workspace.default.data_management` |
| Job fails | Check notebook paths match workspace |

---

## Reproduction Time

- Data prep: 10 minutes
- Upload to Databricks: 5 minutes
- Run all notebooks: 5 minutes
- Create dashboard: 10 minutes
- Total: **~30 minutes**

---

## Next Steps

1. ✅ Clone/download this repo
2. ✅ Follow setup steps above
3. ✅ Customize for your data
4. ✅ Run pipeline
5. ✅ Create dashboard
6. ✅ Schedule job
7. ✅ Submit for evaluation

---

## Support

If you encounter issues:
1. Check [TROUBLESHOOTING.md](TROUBLESHOOTING.md)
2. Review [ARCHITECTURE.md](ARCHITECTURE.md)
3. Compare your code with `notebooks/` folder

---

**Setup Complete! ✅ Ready to run the pipeline.**
