# TCGA ETL Pipelines

This directory contains Delta Live Tables (DLT) pipelines for ingesting and transforming TCGA data.

## Pipeline Structure

### Bronze Layer: Raw Data Ingestion (`transformations/data_ingestion.py`)
- `expression_files_info`: Metadata about gene expression files from GDC API
- `cases`: Clinical case information (demographics, diagnoses, exposures)
- `expression_profiles`: Gene expression data (FPKM values) from all samples

### Silver Layer: Cleaned and Transformed Data (`transformations/transform.py`)
- `cases_demographics`: Patient demographic information with quality checks
- `cases_diagnoses`: Diagnosis and treatment information with derived columns
- `cases_exposures`: Environmental exposure information with smoking status

## Data Quality Expectations

Each table includes data quality expectations using DLT's `@dlt.expect` decorators:

- **Validation expectations**: Log warnings when data doesn't meet criteria
- **Drop expectations**: Drop records that don't meet critical criteria (e.g., missing IDs)

## Running the Pipeline

### Using Databricks CLI

```bash
# Create the pipeline
databricks pipelines create \
  --name "TCGA Data Pipeline" \
  --storage "/path/to/storage" \
  --target "your_catalog.tcga" \
  --notebook-libraries '[{"notebook": {"path": "/path/to/data_ingestion.py"}}, {"notebook": {"path": "/path/to/transform.py"}}]'

# Start the pipeline
databricks pipelines start --pipeline-id <pipeline-id>
```

### Using Databricks UI

1. Go to **Workflows** > **Delta Live Tables**
2. Click **Create Pipeline**
3. Configure:
   - **Pipeline name**: TCGA Data Pipeline
   - **Product edition**: Advanced
   - **Notebook libraries**: Add both `data_ingestion.py` and `transform.py`
   - **Target schema**: `<catalog>.tcga`
   - **Storage location**: Choose appropriate location
4. Click **Create**
5. Click **Start** to run the pipeline

## Configuration

The pipeline reads configuration from `config.json` in the project root:

```json
{
  "lakehouse": {
    "catalog": "your_catalog",
    "schema": "tcga",
    "volume": "tcga_files"
  }
}
```

## Features

### Change Data Feed (CDF)
All tables have Change Data Feed enabled for tracking changes over time.

### Auto Optimization
Z-ordering is configured on key columns for improved query performance:
- `expression_files_info`: `file_id`, `case_id`
- `cases`: `case_id`
- `expression_profiles`: `file_id`, `gene_id`
- `cases_demographics`: `case_id`, `file_id`
- `cases_diagnoses`: `case_id`, `file_id`, `tissue_or_organ_of_origin`
- `cases_exposures`: `case_id`, `file_id`

### Monitoring

DLT provides built-in monitoring dashboards showing:
- Pipeline run history and status
- Data quality metrics
- Row counts and data flow
- Expectations pass/fail rates

## Troubleshooting

### Common Issues

1. **Config file not found**: Ensure `config.json` exists at the project root
2. **Volume path errors**: Verify the volume exists and is accessible
3. **Schema mismatches**: Check that raw files match expected schema
4. **Expectation failures**: Review DLT metrics to see which expectations are failing

### Viewing Logs

- In Databricks UI: Navigate to the pipeline run and click on individual table updates
- Using CLI: `databricks pipelines get --pipeline-id <id>`

## Next Steps

After running the pipeline:
1. Verify data quality metrics in DLT dashboard
2. Run analysis notebooks (e.g., `02-tcga-expression-clustering.py`)
3. Create dashboards using the silver tables
4. Set up alerts for data quality issues
