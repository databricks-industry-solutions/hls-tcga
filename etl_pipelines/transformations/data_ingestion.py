"""
TCGA Data Ingestion DLT Pipeline

This module defines Delta Live Tables for ingesting raw TCGA data from Unity Catalog volumes
into managed Delta tables with data quality expectations.

Tables created:
- expression_files_info: Metadata about gene expression files
- cases: Clinical case information
- expression_profiles: Gene expression data from all samples
"""

import dlt
from pyspark.sql.types import *
from pyspark.sql.functions import col, current_timestamp, count, when, isnan

# Load configuration from DLT pipeline settings
# These are set in pipelines.yml configuration section
# Use default values if not set (should not happen in production)
CATALOG = spark.conf.get("catalog", None)
SCHEMA = spark.conf.get("schema", None)
VOLUME = spark.conf.get("volume", None)

# Validate configuration was loaded
if not CATALOG or not SCHEMA or not VOLUME:
    raise ValueError(
        f"DLT Pipeline configuration not loaded correctly!\n"
        f"  catalog: {CATALOG}\n"
        f"  schema: {SCHEMA}\n"
        f"  volume: {VOLUME}\n"
        f"Check pipelines.yml configuration section."
    )

VOLUME_PATH = f'/Volumes/{CATALOG}/{SCHEMA}/{VOLUME}'

print(f"DLT Pipeline Configuration:")
print(f"  Catalog: {CATALOG}")
print(f"  Schema: {SCHEMA}")
print(f"  Volume: {VOLUME}")
print(f"  Volume Path: {VOLUME_PATH}")

# Verify the volume path is accessible (this will fail early if there's an issue)
try:
    # Try to list the volume to ensure it's accessible
    dbutils.fs.ls(VOLUME_PATH)
    print(f"✓ Volume path is accessible")
except Exception as e:
    print(f"✗ Cannot access volume path: {e}")
    print(f"  This suggests a permission or configuration issue")
    raise

# Data quality thresholds
MIN_FILE_SIZE = 1000  # bytes
MIN_GENE_FPKM = 0.0
MAX_GENE_FPKM = 1000000.0

# ============================================================================
# Bronze Tables: Raw data ingestion
# ============================================================================

@dlt.table(
    name="expression_files_info",
    comment="Raw metadata about TCGA gene expression files from GDC API",
    table_properties={
        "quality": "bronze",
        "pipelines.autoOptimize.zOrderCols": "file_id,case_id",
        "delta.enableChangeDataFeed": "true"
    }
)
@dlt.expect_or_drop("valid_file_id", "file_id IS NOT NULL")
@dlt.expect_or_drop("valid_file_size", f"file_size >= {MIN_FILE_SIZE}")
@dlt.expect("valid_data_format", "data_format = 'TSV'")
def expression_files_info():
    """
    Ingest expression file metadata from GDC API download.

    Quality checks:
    - File ID must be present
    - File size must be at least 1KB
    - Data format should be TSV
    """
    return (
        spark.read.csv(
            f'{VOLUME_PATH}/expressions_info.tsv',
            sep='\t',
            header=True,
            inferSchema=True
        )
        .withColumnRenamed('cases.0.case_id', 'case_id')
        .withColumn('ingestion_timestamp', current_timestamp())
    )


@dlt.table(
    name="cases",
    comment="Raw clinical case information including demographics, diagnoses, and exposures from TCGA",
    table_properties={
        "quality": "bronze",
        "pipelines.autoOptimize.zOrderCols": "case_id",
        "delta.enableChangeDataFeed": "true"
    }
)
@dlt.expect_or_drop("valid_case_id", "case_id IS NOT NULL")
def cases():
    """
    Ingest clinical case data from GDC API download.

    Quality checks:
    - Case ID must be present
    """
    return (
        spark.read.csv(
            f'{VOLUME_PATH}/cases.tsv',
            sep='\t',
            header=True,
            inferSchema=True
        )
        .withColumn('ingestion_timestamp', current_timestamp())
    )


@dlt.table(
    name="expression_profiles",
    comment="Gene expression profiles (FPKM values) for all TCGA samples",
    table_properties={
        "quality": "bronze",
        "pipelines.autoOptimize.zOrderCols": "file_id,gene_id",
        "delta.enableChangeDataFeed": "true"
    }
)
@dlt.expect_or_drop("valid_gene_id", "gene_id IS NOT NULL AND gene_id != ''")
@dlt.expect_or_drop("valid_file_id", "file_id IS NOT NULL")
@dlt.expect("valid_fpkm_range", f"fpkm_unstranded >= {MIN_GENE_FPKM} AND fpkm_unstranded <= {MAX_GENE_FPKM}")
@dlt.expect("valid_gene_name", "gene_name IS NOT NULL")
def expression_profiles():
    """
    Ingest gene expression profiles from individual sample files.

    Schema defines expected columns with proper data types.
    Filters out invalid gene IDs and metadata rows.

    Quality checks:
    - Gene ID must be present and non-empty
    - File ID must be present
    - FPKM values should be in reasonable range (0 to 1M)
    - Gene name should be present
    """
    schema = StructType([
        StructField('gene_id', StringType(), False),
        StructField('gene_name', StringType(), True),
        StructField('gene_type', StringType(), True),
        StructField('unstranded', IntegerType(), True),
        StructField('stranded_first', IntegerType(), True),
        StructField('stranded_second', DoubleType(), True),
        StructField('tpm_unstranded', DoubleType(), True),
        StructField('fpkm_unstranded', DoubleType(), True),
        StructField('fpkm_uq_unstranded', DoubleType(), True),
    ])

    return (
        spark.read.csv(
            f'{VOLUME_PATH}/expressions',
            comment="#",
            sep="\t",
            schema=schema
        )
        .selectExpr('*', '_metadata.file_name as file_id')
        .filter(
            ~col('gene_id').rlike('N_') &
            ~col('gene_id').contains('gene_id')
        )
        .withColumn('ingestion_timestamp', current_timestamp())
    )

