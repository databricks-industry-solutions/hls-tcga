# Databricks notebook source
"""
Create Bronze Layer - Raw Delta Tables

This notebook creates the bronze (raw) layer by ingesting downloaded files
directly into Delta tables with no transformations or data quality checks.
The DLT pipeline will then transform these into silver/gold layers.
"""

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
from pyspark.sql.functions import col, current_timestamp

# Get parameters
dbutils.widgets.text("catalog", "kermany", "Catalog")
dbutils.widgets.text("schema", "tcga", "Schema")
dbutils.widgets.text("volume", "tcga_files", "Volume")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
volume = dbutils.widgets.get("volume")

volume_path = f'/Volumes/{catalog}/{schema}/{volume}'

print(f"Creating Bronze Layer (Raw) tables in {catalog}.{schema}")
print(f"Reading from {volume_path}")
print("Note: No data quality checks or transformations at this stage")

# COMMAND ----------

# Drop existing tables if they exist (both bronze and silver for clean slate)
print("\n1. Dropping existing tables if they exist...")
# Drop bronze tables
spark.sql(f"DROP TABLE IF EXISTS {catalog}.{schema}.expression_files_info_bronze")
spark.sql(f"DROP TABLE IF EXISTS {catalog}.{schema}.cases_bronze")
spark.sql(f"DROP TABLE IF EXISTS {catalog}.{schema}.expression_profiles_bronze")
# Drop silver tables (for DLT pipeline to recreate)
spark.sql(f"DROP TABLE IF EXISTS {catalog}.{schema}.expression_files_info")
spark.sql(f"DROP TABLE IF EXISTS {catalog}.{schema}.cases")
spark.sql(f"DROP TABLE IF EXISTS {catalog}.{schema}.expression_profiles")
print("✓ Cleaned up existing bronze and silver tables")

# COMMAND ----------

# Create expression_files_info_bronze table (raw data, no quality checks)
print("\n2. Creating expression_files_info_bronze table...")
df_files_info = (
    spark.read.csv(
        f'{volume_path}/expressions_info.tsv',
        sep='\t',
        header=True,
        inferSchema=True
    )
    .withColumnRenamed('cases.0.case_id', 'case_id')
    .withColumn('ingestion_timestamp', current_timestamp())
)

count_files = df_files_info.count()
df_files_info.write.format("delta").mode("overwrite").saveAsTable(f"{catalog}.{schema}.expression_files_info_bronze")
print(f"✓ Created {catalog}.{schema}.expression_files_info_bronze with {count_files} rows")

# COMMAND ----------

# Create cases_bronze table (raw data, no quality checks)
print("\n3. Creating cases_bronze table...")
df_cases = (
    spark.read.csv(
        f'{volume_path}/cases.tsv',
        sep='\t',
        header=True,
        inferSchema=True
    )
    .withColumn('ingestion_timestamp', current_timestamp())
)

count_cases = df_cases.count()
df_cases.write.format("delta").mode("overwrite").saveAsTable(f"{catalog}.{schema}.cases_bronze")
print(f"✓ Created {catalog}.{schema}.cases_bronze with {count_cases} rows")

# COMMAND ----------

# Create expression_profiles_bronze table (raw data, optimized for large dataset)
print("\n4. Creating expression_profiles_bronze table...")
print("   Note: This table is large and may take several minutes...")

expr_schema = StructType([
    StructField('gene_id', StringType(), True),
    StructField('gene_name', StringType(), True),
    StructField('gene_type', StringType(), True),
    StructField('unstranded', IntegerType(), True),
    StructField('stranded_first', IntegerType(), True),
    StructField('stranded_second', DoubleType(), True),
    StructField('tpm_unstranded', DoubleType(), True),
    StructField('fpkm_unstranded', DoubleType(), True),
    StructField('fpkm_uq_unstranded', DoubleType(), True),
])

# Read and process with optimizations
df_expressions = (
    spark.read.csv(
        f'{volume_path}/expressions',
        comment="#",
        sep="\t",
        schema=expr_schema
    )
    .selectExpr('*', '_metadata.file_name as file_id')
    .withColumn('ingestion_timestamp', current_timestamp())
    .repartition(200)  # Optimize partitioning for large dataset
)

# Write with optimizations enabled
print("   Writing to Delta table (this may take a while)...")
(df_expressions.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .option("optimizeWrite", "true")
    .option("mergeSchema", "false")
    .saveAsTable(f"{catalog}.{schema}.expression_profiles_bronze"))

# Count rows after writing (faster than counting before)
count_expr = spark.table(f"{catalog}.{schema}.expression_profiles_bronze").count()
print(f"✓ Created {catalog}.{schema}.expression_profiles_bronze with {count_expr} rows")

# COMMAND ----------

# Final summary
print("\n" + "="*70)
print("SUCCESS: Bronze Layer (Raw) created successfully!")
print("="*70)
print(f"\nBronze tables created in {catalog}.{schema}:")
print(f"  - expression_files_info_bronze: {count_files:,} rows")
print(f"  - cases_bronze: {count_cases:,} rows")
print(f"  - expression_profiles_bronze: {count_expr:,} rows")
print(f"\nTotal rows ingested: {count_files + count_cases + count_expr:,}")
print("\n✅ Bronze layer ready for DLT transformation pipeline!")
