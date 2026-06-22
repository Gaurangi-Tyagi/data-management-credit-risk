# Credit Risk Analysis Dashboard
### Data Management 2 — SRH University Hamburg

---

## Project Overview

This project implements a Medallion Architecture data pipeline in Databricks that combines Home Credit loan application data with World Bank economic indicators to build a credit risk analysis dashboard. The pipeline uses Auto Loader for incremental batch ingestion, with a scheduled job orchestrating all three layers automatically.

**Business Objectives:**
1. Segment loan applicants into Low, Medium, and High risk tiers based on credit-to-income ratio
2. Analyze default rates across risk segments, family status, and education type
3. Enrich credit risk analysis with macroeconomic context by joining World Bank GDP data

---

## Data Sources

### Source 1: Home Credit Default Risk (CSV)
- **URL:** https://www.kaggle.com/datasets/megancrenshaw/home-credit-default-risk
- **Files:** `batch_1.csv` (200,000 rows), `batch_2.csv` (107,511 rows)
- **Total:** 307,511 rows x 122 columns
- **Location:** `/Volumes/workspace/default/data_management/batches/`
- **Ingestion:** Databricks Auto Loader (`cloudFiles`) — detects and processes new files automatically
- **Key Columns:** SK_ID_CURR, TARGET, AMT_CREDIT, AMT_INCOME_TOTAL, DAYS_EMPLOYED

### Source 2: World Bank GDP Data (Live API)
- **API:** `https://api.worldbank.org/v2/country/{country}/indicator/NY.GDP.MKTP.CD?format=json`
- **Countries:** DEU, USA, GBR, FRA, IND, BRA, CAN, JPN, CHN, AUS
- **Format:** JSON fetched live on each pipeline run
- **Key Metrics:** GDP (USD), Year, Country Code

---

## Architecture

```
DATABRICKS WORKSPACE
└── CATALOG: workspace
    └── DATABASE: dm2_credit_risk
        ├── BRONZE LAYER
        │   ├── bronze_loan_apps       (CSV batches via Auto Loader)
        │   └── bronze_world_bank      (live World Bank API)
        │
        ├── SILVER LAYER
        │   ├── silver_loan_apps       (cleaned, filtered, type-cast)
        │   └── silver_world_bank      (standardized, filtered years >= 2018)
        │
        └── GOLD LAYER
            └── gold_credit_risk_metrics  (aggregated metrics + World Bank GDP join)
```

---

## Implementation

### Bronze Layer

The Bronze layer ingests raw data from both sources. Home Credit CSV data is loaded incrementally using Databricks Auto Loader, which tracks processed files via a checkpoint and appends only new batches on each run. World Bank GDP data is fetched live from the API on every pipeline execution.

```python
# Auto Loader — incremental CSV ingestion
df_bronze_csv = (spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "csv")
    .option("cloudFiles.schemaLocation", "/Volumes/workspace/default/data_management/checkpoints/schema")
    .option("header", "true")
    .option("inferSchema", "true")
    .load("/Volumes/workspace/default/data_management/batches/")
)

(df_bronze_csv.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", "/Volumes/workspace/default/data_management/checkpoints/bronze")
    .option("mergeSchema", "true")
    .trigger(once=True)
    .toTable("dm2_credit_risk.bronze_loan_apps")
)
```

```python
# World Bank API — live fetch on each run
import requests

countries = ["DEU", "USA", "GBR", "FRA", "IND", "BRA", "CAN", "JPN", "CHN", "AUS"]
all_data = []

for country in countries:
    url = f"https://api.worldbank.org/v2/country/{country}/indicator/NY.GDP.MKTP.CD?format=json&per_page=10"
    response = requests.get(url)
    records = response.json()[1]
    if records:
        for r in records:
            all_data.append({
                "country": r["countryiso3code"],
                "country_name": r["country"]["value"],
                "year": r["date"],
                "gdp": float(r["value"]) if r["value"] is not None else None
            })

df_bronze_json = spark.createDataFrame(all_data)
df_bronze_json.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("dm2_credit_risk.bronze_world_bank")
```

---

### Silver Layer

The Silver layer cleans and transforms both data sources. Since Auto Loader reads CSV columns as strings, all numeric columns are explicitly cast before any calculations are applied.

**Loan Applications:**
- Filter null SK_ID_CURR and TARGET
- Cast AMT_CREDIT and AMT_INCOME_TOTAL to double
- Handle missing values with coalesce
- Calculate credit_to_income_ratio
- Add processing_date timestamp

**World Bank Data:**
- Filter null GDP and year values
- Cast year to integer, GDP to double
- Filter to years >= 2018
- Standardize country codes to uppercase
- Add processing_date timestamp

---

### Gold Layer

The Gold layer creates business-ready metrics by aggregating loan application data and joining it with World Bank GDP. Since the Home Credit dataset does not contain country information, applicants are assigned a country based on their income level. The latest available GDP figure for each assigned country is then joined to provide macroeconomic context.

```sql
CREATE OR REPLACE TABLE dm2_credit_risk.gold_credit_risk_metrics AS
WITH loan_with_country AS (
    SELECT *,
        CASE 
            WHEN AMT_INCOME_TOTAL > 200000 THEN 'USA'
            WHEN AMT_INCOME_TOTAL > 150000 THEN 'DEU'
            WHEN AMT_INCOME_TOTAL > 100000 THEN 'GBR'
            WHEN AMT_INCOME_TOTAL > 70000  THEN 'FRA'
            WHEN AMT_INCOME_TOTAL > 50000  THEN 'BRA'
            WHEN AMT_INCOME_TOTAL > 30000  THEN 'CHN'
            ELSE 'IND'
        END as assigned_country
    FROM dm2_credit_risk.silver_loan_apps
),
latest_gdp AS (
    SELECT country_code, gdp_usd
    FROM dm2_credit_risk.silver_world_bank
    WHERE year = (SELECT MAX(year) FROM dm2_credit_risk.silver_world_bank)
)
SELECT 
    CASE 
        WHEN l.credit_to_income_ratio > 3   THEN 'High Risk'
        WHEN l.credit_to_income_ratio > 1.5 THEN 'Medium Risk'
        ELSE 'Low Risk'
    END as risk_segment,
    l.family_status,
    l.education_type,
    l.assigned_country,
    g.gdp_usd as country_gdp_usd,
    COUNT(l.SK_ID_CURR) as total_customers,
    SUM(CASE WHEN l.TARGET = 1 THEN 1 ELSE 0 END) as defaulted_customers,
    ROUND(SUM(CASE WHEN l.TARGET = 1 THEN 1 ELSE 0 END) / COUNT(l.SK_ID_CURR) * 100, 2) as default_rate_percent,
    ROUND(AVG(l.AMT_CREDIT), 2) as avg_credit_amount,
    ROUND(AVG(l.AMT_INCOME_TOTAL), 2) as avg_income,
    ROUND(AVG(l.credit_to_income_ratio), 2) as avg_credit_to_income_ratio,
    COUNT(DISTINCT l.SK_ID_CURR) as unique_customers
FROM loan_with_country l
LEFT JOIN latest_gdp g ON l.assigned_country = g.country_code
GROUP BY risk_segment, l.family_status, l.education_type, l.assigned_country, g.gdp_usd
ORDER BY default_rate_percent DESC;
```

---

## Dashboard

**Name:** `Credit_Risk_Dashboard`

9 SQL queries were written and tested during development. 5 were used in the final dashboard. The remaining queries were exploratory and excluded from the final layout.

### Query 1: Default Rate by Risk Segment
```sql
SELECT 
    risk_segment,
    default_rate_percent
FROM dm2_credit_risk.gold_credit_risk_metrics
GROUP BY risk_segment, default_rate_percent
ORDER BY default_rate_percent DESC;
```

### Query 2: Customer Count by Risk Segment
```sql
SELECT 
    risk_segment,
    SUM(total_customers) as customer_count
FROM dm2_credit_risk.gold_credit_risk_metrics
GROUP BY risk_segment
ORDER BY customer_count DESC;
```

### Query 3: Defaulted vs Non-Defaulted
```sql
SELECT 
    CASE 
        WHEN defaulted_customers > 0 THEN 'Defaulted'
        ELSE 'Active'
    END as status,
    SUM(total_customers) as count
FROM dm2_credit_risk.gold_credit_risk_metrics
GROUP BY status;
```

### Query 4: Average Income by Risk Segment and Country
```sql
SELECT 
    risk_segment,
    assigned_country,
    ROUND(AVG(avg_income), 0) as avg_income,
    ROUND(AVG(country_gdp_usd), 0) as country_gdp,
    ROUND(AVG(avg_income) / AVG(country_gdp_usd) * 100, 4) as income_as_pct_of_gdp
FROM dm2_credit_risk.gold_credit_risk_metrics
GROUP BY risk_segment, assigned_country
ORDER BY avg_income DESC;
```

### Query 5: Credit to Income Ratio by Country and Risk
```sql
SELECT 
    assigned_country,
    risk_segment,
    ROUND(AVG(avg_credit_to_income_ratio), 2) as avg_credit_ratio,
    ROUND(AVG(country_gdp_usd) / 1000000000, 2) as gdp_billions,
    SUM(total_customers) as total_customers
FROM dm2_credit_risk.gold_credit_risk_metrics
GROUP BY assigned_country, risk_segment
ORDER BY gdp_billions DESC;
```

---

## Scheduled Job

**Job Name:** `dm2_credit_risk_batch_job`

The job runs all three layers in sequence. When a new batch file is added to the `/batches/` folder, Auto Loader detects it on the next run and appends the records through the full pipeline, refreshing the dashboard automatically.

| Task | Notebook | Depends On |
|------|----------|------------|
| run_bronze_layer | Bronze_Layer | — |
| run_Silver_Layer | Silver_Layer | run_bronze_layer |
| run_gold_layer | Gold_Layer | run_Silver_Layer |

**Schedule:** Daily at 7:09 PM

**Run History:**

| Date | Triggered By | Duration | Status |
|------|-------------|----------|--------|
| Jun 21, 2026, 07:09 PM | Scheduler | 1m 9s | Successful |
| Jun 22, 2026, 07:09 AM | Scheduler | 2s | Failed (notebook path) |
| Jun 22, 2026, 10:12 AM | Manual | 1s | Failed (notebook path) |
| Jun 22, 2026, 10:20 AM | Manual | 1m 58s | Successful |

---

## Key Findings

- Overall default rate: 8.07% across 307,511 customers
- High-risk customers (credit-to-income ratio > 3) show the highest default rates
- Family status and education type influence default probability
- Higher-income applicants map to higher-GDP economies (USA, DEU, GBR); lower-income applicants map to emerging markets (IND, CHN, BRA)
- The dashboard did not show significant metric changes between the two batch runs since the data was split from the same source dataset. In a production environment with genuinely new loan applications arriving daily, the pipeline is designed to reflect those changes automatically.

---

## Technical Stack

| Component | Technology |
|-----------|------------|
| Platform | Databricks (Free Edition) |
| Compute | Serverless SQL Warehouse (2XS) |
| Storage | Delta Lake (Volumes) |
| Ingestion | Auto Loader (cloudFiles) |
| Data Formats | CSV, JSON, Delta |
| Languages | SQL, PySpark |
| Architecture | Medallion (Bronze → Silver → Gold) |
| Orchestration | Databricks Jobs & Pipelines |

---

## Errors Encountered

| Error | Cause | Fix |
|-------|-------|-----|
| `NOT_SUPPORTED_WITH_SERVERLESS` | Cache/persist not supported | Removed cache operations |
| `corrupt_record` in JSON | Multi-line JSON array | Added `option("multiLine", "true")` |
| `DAYS_EMPLOYMENT not found` | Incorrect column name | Corrected to `DAYS_EMPLOYED` |
| `NameError: df_silver_csv` | Variables don't persist across notebooks | Read from Delta tables in each notebook |
| `CF_EMPTY_DIR_FOR_SCHEMA_INFERENCE` | Auto Loader folder was empty | Uploaded batch file before running |
| `DELTA_METADATA_MISMATCH` | Schema mismatch on existing table | Dropped table and added `mergeSchema` |
| `BINARY_OP_WRONG_TYPE` | Auto Loader reads CSV columns as STRING | Explicitly cast columns to double |
| Notebook path not found in job | Notebooks located in a subfolder | Updated all 3 task paths in job config |

---

## File Structure

```
/Volumes/workspace/default/data_management/
├── batches/
│   ├── batch_1.csv              # First 200,000 rows
│   └── batch_2.csv              # Remaining 107,511 rows
└── checkpoints/
    ├── schema/                  # Auto Loader schema checkpoint
    └── bronze/                  # Auto Loader processing checkpoint

/Workspace/Users/gaurangi12tyagi@gmail.com/Dashboard/Layer_Notebook/
├── Bronze_Layer
├── Silver_Layer
└── Gold_Layer

Databases:
└── dm2_credit_risk
    ├── bronze_loan_apps
    ├── bronze_world_bank
    ├── silver_loan_apps
    ├── silver_world_bank
    └── gold_credit_risk_metrics
```

---

## Data Dictionary

| Column | Type | Description |
|--------|------|-------------|
| risk_segment | STRING | Risk category: Low, Medium, or High |
| family_status | STRING | Applicant family status |
| education_type | STRING | Applicant educational background |
| assigned_country | STRING | Country assigned based on income level |
| country_gdp_usd | DOUBLE | Latest World Bank GDP for assigned country |
| total_customers | BIGINT | Total customers in segment |
| defaulted_customers | BIGINT | Count of defaulted loans |
| default_rate_percent | DOUBLE | Default rate as a percentage |
| avg_credit_amount | DOUBLE | Average credit amount (USD) |
| avg_income | DOUBLE | Average annual income (USD) |
| avg_credit_to_income_ratio | DOUBLE | Average credit-to-income ratio |
| unique_customers | BIGINT | Count of unique customers |

---

## Future Enhancements

1. Connect to a live loan application source so each batch contains genuinely new records
2. Add time-series tracking of default trends as batches accumulate
3. Build a predictive default model using XGBoost on the Gold layer
4. Replace batch Auto Loader with Kafka-based streaming for real-time ingestion
5. Add automated alerts for high-risk portfolio threshold breaches

---

## AI Assistance

AI tools were used selectively to support development. Specifically, AI was used for debugging pipeline errors, clarifying PySpark syntax (coalesce, when, cast, Auto Loader stream options), navigating the Databricks interface, drafting and structuring this README, and providing guidance on the overall pipeline architecture. All final decisions on data sources, business logic, implementation, and project structure were independently determined and executed.

---

## References

- Databricks Documentation: https://docs.databricks.com
- Databricks Auto Loader: https://docs.databricks.com/ingestion/auto-loader/index.html
- Medallion Architecture: https://databricks.com/glossary/medallion-architecture
- Home Credit Dataset: https://www.kaggle.com/datasets/megancrenshaw/home-credit-default-risk
- World Bank API: https://data.worldbank.org/

---

## Screenshots

### Data Exploration

![Data Exploration](ADD_GITHUB_LINK_HERE: Data_Management_1.png)

### Workspace Structure

![Workspace](ADD_GITHUB_LINK_HERE: File_structure.png)

### Dashboard

![Dashboards Page](ADD_GITHUB_LINK_HERE: Dashboard_Databricks.png)

![Dashboard View](ADD_GITHUB_LINK_HERE: Dashboard_1.png)

![Dashboard Datasets](ADD_GITHUB_LINK_HERE: Dashboard_data.png)

### Scheduled Job

![Jobs Page](ADD_GITHUB_LINK_HERE: Scheduled_Job.png)

![Run History](ADD_GITHUB_LINK_HERE: Scheduled_Job_2.png)

![Pipeline Tasks](ADD_GITHUB_LINK_HERE: Task.png)
