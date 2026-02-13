# Data Intelligence for R&D: The Cancer Genome Atlas (TCGA)

<img src="https://www.cancer.gov/ccg/sites/g/files/xnrzdm256/files/styles/cgov_featured/public/cgov_image/media_image/2022-06/TCGA%20people%20and%20layers%20of%20data%20425x319.jpg?h=982f41e1&itok=zkQ_l8-t" width=500 >

[The Cancer Genome Atlas (TCGA)](https://www.cancer.gov/ccg/research/genome-sequencing/tcga) is a comprehensive initiative to understand the molecular foundations of cancer through genome analysis. TCGA contains over 2.5 petabytes of genomic data spanning 33 cancer types from over 10,000 patients.

## Quick Start

### Recommended: One-Command Deployment

```bash
# Clone the repository
git clone <repository-url>
cd hls-tcga

# Run the deployment script (interactive setup)
python deploy.py

# Or deploy and run immediately
python deploy.py --run
```

The `deploy.py` script will:
- Auto-detect your Databricks workspace and cloud provider
- Prompt for configuration with intelligent defaults
- Create Unity Catalog resources (catalog, schema, volume)
- Deploy the complete pipeline to your workspace
- Optionally run the workflow immediately

### Alternative: Direct Bundle Deployment

```bash
# Configure Databricks CLI
databricks configure --token

# Create configuration
python deploy.py --config-only

# Deploy to development
databricks bundle deploy

# Run the workflow
databricks bundle run tcga_data_workflow
```

## Architecture

### Medallion Data Pipeline

```
Bronze (Raw) → Silver (Transformed) → Gold (Analytics)
```

**Bronze Layer** - Raw data ingestion
- Creates raw Delta tables from GDC API downloads
- Stores data in Unity Catalog volumes
- No transformations or quality checks

**Silver Layer** - Delta Live Tables (DLT) with quality checks
- `expression_files_info`: File metadata with validation
- `cases`: Clinical case information
- `expression_profiles`: Gene expression data with FPKM validation
- `cases_demographics`: Patient demographics with vital status
- `cases_diagnoses`: Tumor classification and treatments
- `cases_exposures`: Environmental factors (smoking, alcohol)

**Gold Layer** - ML Analysis
- PCA and t-SNE dimensionality reduction
- K-means clustering with MLflow tracking
- Interactive Plotly visualizations

### Workflow Components

1. **Configuration** (`00-setup.ipynb`, `config.json`)
   - Unity Catalog setup
   - Environment-specific configuration

2. **Data Download** (`01-data-download.py`)
   - Fetches data from [GDC APIs](https://gdc.cancer.gov/access-data/gdc-data-transfer-tool)
   - Parallel downloads (64 concurrent workers)
   - Stores in Unity Catalog volumes

3. **Bronze Layer** (`03-create-bronze-layer.py`)
   - Creates raw Delta tables from downloaded files
   - No transformations or quality checks

4. **Silver Layer** (`etl_pipelines/transformations/transform.py`)
   - Delta Live Tables with data quality expectations
   - Transforms bronze tables into clean silver tables
   - Auto-optimization with Z-ordering and Change Data Feed

5. **Analysis** (`02-tcga-expression-clustering-optimized.py`)
   - Memory-efficient processing
   - MLflow experiment tracking
   - Interactive visualizations

## Project Structure

```
hls-tcga/
├── databricks.yml                    # Bundle configuration
├── config.json                       # Runtime configuration
├── deploy.py                         # Interactive deployment script
├── resources/
│   ├── pipelines.yml                # DLT pipeline definitions
│   └── jobs.yml                     # Job workflow definitions
├── etl_pipelines/
│   └── transformations/
│       └── transform.py             # DLT silver layer transformations
├── utils/
│   ├── config_loader.py            # Configuration utilities
│   └── __init__.py
├── 00-setup.ipynb                   # Setup notebook
├── 00-load-config.py               # Config loader for jobs
├── 01-data-download.py             # Data download
├── 02-tcga-expression-clustering-optimized.py  # ML analysis
├── 03-create-bronze-layer.py       # Bronze layer creation
└── tcga_dashboard.lvdash.json      # Lakeview dashboard
```

## Key Features

### Production-Ready
- **Serverless DLT**: Unity Catalog enabled, no cluster management
- **On-Demand Instances**: No spot termination issues
- **Data Quality**: DLT expectations for validation
- **MLflow Integration**: Experiment tracking for reproducibility
- **Medallion Architecture**: Bronze → Silver → Gold layers

### Scalable & Efficient
- **Parallel Downloads**: 64 concurrent workers
- **Memory Optimization**: Validates data size before pandas conversion
- **Delta Lake**: ACID transactions, time travel, Change Data Feed
- **Auto-Optimization**: Z-ordering for query performance

### Developer-Friendly
- **One-Command Deploy**: `python deploy.py`
- **Environment Management**: Dev/staging/prod configurations
- **Comprehensive Logging**: Progress tracking and error reporting
- **Widget Parameters**: Runtime configuration without code changes

## Data Pipeline Details

### GDC API Endpoints

```
Cases:  https://api.gdc.cancer.gov/cases
Files:  https://api.gdc.cancer.gov/files
Data:   https://api.gdc.cancer.gov/data
```

### Tables Created

**Bronze** (raw):
- `expression_files_info_bronze`
- `cases_bronze`
- `expression_profiles_bronze`

**Silver** (cleaned & validated):
- `expression_files_info`
- `cases`
- `expression_profiles`
- `cases_demographics`
- `cases_diagnoses`
- `cases_exposures`

**Gold** (analytics):
- `{user}_expression_profiles_pivoted`
- `{user}_expression_dimensionality_results`
- `{user}_expression_clustering_final`

## Configuration

### Runtime Parameters

Configurable via Databricks widgets:
- `catalog`: Unity Catalog name (default: "kermany")
- `schema`: Schema name (default: "tcga")
- `volume`: Volume name (default: "tcga_files")
- `max_workers`: Concurrent downloads (default: 64)
- `force_download`: Re-download existing files (default: false)
- `n_top_genes`: Variable genes for analysis (default: 1000)
- `n_pca_components`: PCA dimensions (default: 50)

### Environment Variables

```bash
# Override defaults
export TCGA_CATALOG=my_catalog
export TCGA_SCHEMA=my_schema
export TCGA_MAX_WORKERS=128
```

## Documentation

- **[DEPLOYMENT.md](./DEPLOYMENT.md)**: Deployment guide
- **[resources/README.md](./resources/README.md)**: Bundle configuration
- **[etl_pipelines/README.md](./etl_pipelines/README.md)**: DLT pipeline details
- **[utils/README.md](./utils/README.md)**: Utility functions

## Requirements

- Databricks workspace (AWS or Azure)
- Unity Catalog enabled
- Databricks CLI configured
- Python 3.8+

## License

Part of Databricks Solution Accelerators.

## Acknowledgments

- Data from [The Cancer Genome Atlas (TCGA)](https://www.cancer.gov/ccg/research/genome-sequencing/tcga)
- APIs from [Genomic Data Commons (GDC)](https://gdc.cancer.gov/)
