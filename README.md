# Data Intelligence for R&D: The Cancer Genome Atlas (TCGA)

<img src="https://www.cancer.gov/ccg/sites/g/files/xnrzdm256/files/styles/cgov_featured/public/cgov_image/media_image/2022-06/TCGA%20people%20and%20layers%20of%20data%20425x319.jpg?h=982f41e1&itok=zkQ_l8-t" width=500 >


[The Cancer Genome Atlas (TCGA)](https://www.cancer.gov/ccg/research/genome-sequencing/tcga) represents a comprehensive and coordinated initiative aimed at expediting our understanding of the molecular foundations of cancer by leveraging genome analysis technologies, including large-scale genome sequencing. Spearheaded in 2006 by the [National Cancer Institute (NCI)](https://www.nih.gov/about-nih/what-we-do/nih-almanac/national-cancer-institute-nci) and [National Human Genome Research Institute (NHGRI)](https://www.genome.gov/), TCGA has set forth the following objectives:

1. Enhance our capacity for cancer diagnosis, treatment, and prevention by delving into the genomic alterations in cancer, which will pave the way for more refined diagnostic and therapeutic strategies.
2. Pinpoint molecular therapy targets by discerning common molecular traits of tumors, enabling the development of treatments that specifically target these markers.
3. Uncover carcinogenesis mechanisms by identifying the genomic shifts that lead to the transition of normal cells into tumors.
4. Strengthen predictions of cancer recurrence by understanding the genomic modifications in tumors, facilitating the recognition of indicators that signify an increased likelihood of cancer resurgence post-treatment.
5. Foster new breakthroughs via data sharing. TCGA has adopted a policy of sharing all its data and findings with the global scientific fraternity, promoting independent research, novel discoveries, and the development of improved solutions.

TCGA boasts over 2.5 petabytes of genomic, epigenomic, transcriptomic, and proteomic data spanning 33 cancer types. Contributions from over 10,000 patients include tumor samples and matched controls from blood or nearby normal tissues. The Genomic Data Commons offers complete access to this data, and users can visually navigate it using the Integrated Genomics Viewer.

TCGA serves as a comprehensive repository of pivotal genomic variations in major cancers, continually propelling significant advancements in cancer biology comprehension. It illuminates the mechanisms underlying tumorigenesis and sets the stage for the next generation of diagnostic and therapeutic methods.

## ✨ New: Production-Ready Features

This solution accelerator has been enhanced with Databricks best practices including:

- ✅ **Databricks Asset Bundle (DAB)**: One-command deployment to dev/staging/prod
- ✅ **Configuration Management**: Environment-specific configs with validation
- ✅ **Data Quality Checks**: DLT expectations for data validation
- ✅ **Error Handling**: Retry logic, dead letter queue, notifications
- ✅ **MLflow Integration**: Experiment tracking for analysis notebooks
- ✅ **Optimized Processing**: Memory-efficient Spark operations
- ✅ **Comprehensive Logging**: Structured logging with progress tracking

See [IMPROVEMENTS.md](./IMPROVEMENTS.md) for detailed documentation of all enhancements.

## Workflow Overview

Within this solution accelerator, we present a production-ready template for loading RNA expression profiles from [TCGA](https://portal.gdc.cancer.gov/) and associated clinical data into the Databricks platform, with comprehensive data engineering best practices. The workflow demonstrates how to construct a database of gene expression profiles combined with pertinent metadata and manage all data assets using [Unity Catalog (UC)](https://www.databricks.com/product/unity-catalog).

### Quick Start

#### Option 1: One-Command Deployment (Recommended)

The easiest way to deploy the complete pipeline:

```bash
# Clone the repository
git clone <repository-url>
cd hls-tcga

# Run the deployment script (interactive)
python deploy.py

# Or deploy and run immediately
python deploy.py --run
```

The `deploy.py` script will:
- Auto-detect your Databricks workspace and cloud provider (AWS/Azure)
- Prompt for configuration parameters with intelligent defaults
- Create Unity Catalog resources (catalog, schema, volume)
- Deploy the complete pipeline to your workspace
- Optionally run the workflow immediately

📖 See [DEPLOYMENT.md](./DEPLOYMENT.md) for detailed deployment options and configuration.

#### Option 2: Manual Databricks Asset Bundle

If you prefer direct Databricks CLI usage:

```bash
# Configure Databricks CLI
databricks configure --token

# Create config.json with your settings
python deploy.py --config-only

# Deploy to development
databricks bundle deploy --target dev

# Run the complete workflow
databricks bundle run tcga_data_workflow --target dev
```

#### Option 3: Manual Notebook Execution

1. **Configure**: Run [00-setup.ipynb](./00-setup.ipynb) to create Unity Catalog resources and config.json
2. **Download**: Run [01-data-download.py](./01-data-download.py) to fetch data from GDC APIs
3. **Transform**: Deploy [DLT pipelines](./etl_pipelines) to create managed tables
4. **Analyze**: Run [02-tcga-expression-clustering-optimized.py](./02-tcga-expression-clustering-optimized.py) for analysis

### Workflow Components

#### 1. Configuration Management
- Environment-specific configs (dev/staging/prod) in [config/](./config/)
- Validation and type safety with `ConfigManager`
- Runtime parameterization with Databricks widgets

#### 2. Data Download ([01-data-download.py](./01-data-download.py))
- Fetches data from [GDC APIs](https://gdc.cancer.gov/access-data/gdc-data-transfer-tool)
- Stores in [Unity Catalog volumes](https://docs.databricks.com/en/data-governance/unity-catalog/create-volumes.html)
- Parallel downloads with retry logic and progress tracking
- Comprehensive error handling and logging

#### 3. ETL Pipelines ([etl_pipelines/](./etl_pipelines/))
- **Bronze Layer**: Raw data ingestion with schema validation
- **Silver Layer**: Cleaned and transformed data with derived columns
- [Delta Live Tables](https://www.databricks.com/product/data-engineering/lakeflow-declarative-pipelines) with data quality expectations
- Auto-optimization with Z-ordering and Change Data Feed

#### 4. Analysis ([02-tcga-expression-clustering-optimized.py](./02-tcga-expression-clustering-optimized.py))
- Memory-efficient processing (keeps data in Spark)
- MLflow experiment tracking
- PCA and t-SNE dimensionality reduction
- K-means clustering with visualization
- Interactive Plotly visualizations 

### Workflow

#### Data Download
We use the following enedpoints to download open access data:

cases_endpt: https://api.gdc.cancer.gov/cases

files_endpt: https://api.gdc.cancer.gov/files

data_endpt:  https://api.gdc.cancer.gov/data

#### ETL
After landing the files in a managed volume, we transform the data into the following tables:

[![](https://mermaid.ink/img/pako:eNqFk1Fr2zAQx7-K0Ah-SUe2tWz1wyB1lFHoYCxbKdjDKNY5FdiSkeTRUvLde7YjW04f6gdb-v9_p9PJuhdaaAE0povFS6YIkUq6mPRDQiL3CDVEMYn23EK0DNV7biTfV2CjEUerMbLm5jnRlTZd3IfLz9fXbOVDJ-IPPLmJKsvyLXKjjQAzQV-TFT4BV0kFk726_HJ1tQlsC4VWYrabb-tkvd0GjAPj5Ay5WbNP2yQaiGP3wddxscjUwfDmkdz9Hizb7gfhb0LOlHtdtTXYQU3SAs_OklLiWf0bNJbCU2PAWqnVYJAaHBfccU_8CpHG6DAclBgGZ3l3Bf4Y7vOeEvsFw5x5v1ouValHO0yYnyXc3KZC8oPSwXoPHa9tayaKpQJq3e9FFl79cbdLD6Agr-A_VHmQxDruPLVDyvK6qd7hxtL7walScnHxHes9baybMeaPsfewuhFG-iNKm1tf-HzuffYw9_18jGdn8cxnYF1SlLDwuYA10iWtwdRcCuy4vmsy2ndTRmMcCih5W7mM4o1DtG3wRgAT0mlDY2daWFLeOr17VgWNS15Z8NAGf4_h9ahCH_RzaO2-w4-vPW4yHw?type=png)](https://mermaid.live/edit#pako:eNqFk1Fr2zAQx7-K0Ah-SUe2tWz1wyB1lFHoYCxbKdjDKNY5FdiSkeTRUvLde7YjW04f6gdb-v9_p9PJuhdaaAE0povFS6YIkUq6mPRDQiL3CDVEMYn23EK0DNV7biTfV2CjEUerMbLm5jnRlTZd3IfLz9fXbOVDJ-IPPLmJKsvyLXKjjQAzQV-TFT4BV0kFk726_HJ1tQlsC4VWYrabb-tkvd0GjAPj5Ay5WbNP2yQaiGP3wddxscjUwfDmkdz9Hizb7gfhb0LOlHtdtTXYQU3SAs_OklLiWf0bNJbCU2PAWqnVYJAaHBfccU_8CpHG6DAclBgGZ3l3Bf4Y7vOeEvsFw5x5v1ouValHO0yYnyXc3KZC8oPSwXoPHa9tayaKpQJq3e9FFl79cbdLD6Agr-A_VHmQxDruPLVDyvK6qd7hxtL7walScnHxHes9baybMeaPsfewuhFG-iNKm1tf-HzuffYw9_18jGdn8cxnYF1SlLDwuYA10iWtwdRcCuy4vmsy2ndTRmMcCih5W7mM4o1DtG3wRgAT0mlDY2daWFLeOr17VgWNS15Z8NAGf4_h9ahCH_RzaO2-w4-vPW4yHw)

## 🚀 Features

### Production-Ready Best Practices

- **Databricks Asset Bundle**: Infrastructure as code for multi-environment deployment
- **Configuration Management**: Type-safe, validated, environment-specific configurations
- **Error Handling**: Retry logic, dead letter queue, comprehensive error tracking
- **Logging**: Structured logging with progress tracking and contextual information
- **Data Quality**: DLT expectations for validation and data quality monitoring
- **Performance**: Optimized Spark operations, Z-ordering, Photon acceleration
- **Monitoring**: MLflow experiment tracking, job notifications, DLT metrics

### Key Capabilities

- **Scalable Data Download**: Parallel downloads with configurable workers (default: 64)
- **Delta Lake Tables**: Managed tables with ACID transactions and time travel
- **Change Data Feed**: Track all changes for audit and lineage
- **Memory Efficiency**: Validates data size before pandas conversion
- **Incremental Processing**: Support for incremental data refresh
- **Interactive Visualizations**: Plotly-based interactive plots
- **Checkpointing**: Save intermediate results for reproducibility

## 📁 Project Structure

```
hls-tcga/
├── databricks.yml                          # Bundle configuration
├── config.json                             # Production configuration
├── config/
│   ├── config_manager.py                  # Configuration management
│   ├── config.dev.json                    # Development config
│   ├── config.staging.json                # Staging config
│   └── README.md                          # Config documentation
├── resources/
│   ├── pipelines.yml                      # DLT pipeline definitions
│   ├── jobs.yml                           # Job workflow definitions
│   └── README.md                          # Deployment guide
├── etl_pipelines/
│   ├── transformations/
│   │   ├── data_ingestion.py             # Bronze layer with quality checks
│   │   └── transform.py                  # Silver layer transformations
│   └── README.md                          # Pipeline documentation
├── utils/
│   ├── error_handling.py                  # Error handling utilities
│   ├── logging_utils.py                   # Logging utilities
│   └── README.md                          # Utils documentation
├── 00-setup.ipynb                         # Setup notebook
├── 01-data-download.py                    # Data download (optimized)
├── 02-tcga-expression-clustering.py       # Original analysis notebook
├── 02-tcga-expression-clustering-optimized.py  # Optimized analysis
├── IMPROVEMENTS.md                        # Detailed changelog
└── README.md                              # This file
```

## 📚 Documentation

- **[IMPROVEMENTS.md](./IMPROVEMENTS.md)**: Comprehensive documentation of all improvements
- **[config/README.md](./config/README.md)**: Configuration management guide
- **[resources/README.md](./resources/README.md)**: DAB deployment guide
- **[etl_pipelines/README.md](./etl_pipelines/README.md)**: DLT pipeline documentation
- **[utils/README.md](./utils/README.md)**: Utilities documentation

## 🔧 Configuration

### Environment Variables

```bash
# Optional: Override configuration
export TCGA_CATALOG=my_catalog
export TCGA_SCHEMA=my_schema
export TCGA_MAX_WORKERS=128
export TCGA_ENVIRONMENT=dev  # or staging, production
```

### Databricks Widgets

Runtime parameters can be configured via widgets:
- `catalog`: Unity Catalog name
- `schema`: Schema name
- `volume`: Volume name
- `max_workers`: Concurrent download workers
- `force_download`: Force re-download of existing files
- `n_top_genes`: Number of variable genes to select
- `n_pca_components`: Number of PCA components

## 📊 Data Pipeline

### Bronze Layer Tables
- `expression_files_info`: File metadata from GDC API
- `cases`: Clinical case information
- `expression_profiles`: Gene expression data

### Silver Layer Tables
- `cases_demographics`: Patient demographics with file linkage
- `cases_diagnoses`: Diagnosis and treatment information
- `cases_exposures`: Environmental exposure data

### Analysis Tables
- `{user}_expression_profiles_pivoted`: Wide-format expression data
- `{user}_expression_dimensionality_results`: PCA and t-SNE results
- `{user}_expression_clustering_final`: Clustering results with metadata

## 🎯 Next Steps

1. **Review Improvements**: Read [IMPROVEMENTS.md](./IMPROVEMENTS.md) for detailed documentation
2. **Configure Environment**: Update [config.json](./config.json) with your catalog
3. **Deploy with DAB**: Follow [resources/README.md](./resources/README.md) for deployment
4. **Run Analysis**: Execute the optimized clustering notebook
5. **Monitor Quality**: Review DLT metrics for data quality

## 🤝 Contributing

Contributions are welcome! Please see [CONTRIBUTING.md](./CONTRIBUTING.md) for guidelines.

## 📄 License

This project is part of Databricks Solution Accelerators.

## 🙏 Acknowledgments

- Created in collaboration with [Databricks Data Science Agent](https://www.databricks.com/blog/introducing-databricks-assistant-data-science-agent)
- Data from [The Cancer Genome Atlas (TCGA)](https://www.cancer.gov/ccg/research/genome-sequencing/tcga)
- APIs from [Genomic Data Commons (GDC)](https://gdc.cancer.gov/)

