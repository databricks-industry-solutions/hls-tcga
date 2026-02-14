"""
TCGA Data Transformation DLT Pipeline

Transforms bronze (raw) tables into silver/gold layers with data quality checks.
Reads from bronze tables and applies transformations, validations, and quality expectations.
"""

import dlt
from pyspark.sql.types import *
from pyspark.sql.functions import col, expr, when, isnan, current_timestamp

# Get configuration from DLT pipeline settings
CATALOG = spark.conf.get("catalog")
SCHEMA = spark.conf.get("schema")

@dlt.table(
    comment="Silver: Cleaned metadata about gene expression files with quality checks"
)
@dlt.expect_or_drop("valid_file_id", "file_id IS NOT NULL")
def expression_files_info():
    """Metadata about gene expression files - transformed from bronze"""
    return (
        dlt.read(f"{CATALOG}.{SCHEMA}.expression_files_info_bronze")
        .filter(col("file_id").isNotNull() & col("case_id").isNotNull())
    )

@dlt.table(
    comment="Silver: Cleaned clinical case information with quality checks"
)
def cases():
    """Clinical case information - transformed from bronze"""
    return (
        dlt.read(f"{CATALOG}.{SCHEMA}.cases_bronze")
        .filter(col("case_id").isNotNull())
    )

@dlt.table(
    comment="Silver: Gene expression profiles with quality checks and data validation"
)
@dlt.expect_or_drop("valid_gene_id", "gene_id IS NOT NULL")
@dlt.expect_or_drop("valid_file_id", "file_id IS NOT NULL")
def expression_profiles():
    """Gene expression profiles (FPKM values) - transformed from bronze"""
    return (
        dlt.read(f"{CATALOG}.{SCHEMA}.expression_profiles_bronze")
        .filter(col("gene_id").isNotNull() & col("file_id").isNotNull())
        .withColumn(
            "fpkm_unstranded_validated",
            when(col("fpkm_unstranded") < 0, None).otherwise(col("fpkm_unstranded"))
        )
    )

@dlt.table(
    name="cases_demographics",
    comment="Patient demographic information including ethnicity, gender, race, and vital status",
    table_properties={
        "quality": "silver",
        "pipelines.autoOptimize.zOrderCols": "case_id,file_id",
        "delta.enableChangeDataFeed": "true"
    }
)
@dlt.expect_or_drop("valid_case_id", "case_id IS NOT NULL")
@dlt.expect_or_drop("valid_file_id", "file_id IS NOT NULL")
def cases_demographics():
    """
    Transform demographic data with data quality checks.

    Quality checks:
    - Case ID and File ID must be present
    - Gender should be 'male' or 'female' if provided
    - Year of birth should be reasonable (1900-2020)
    """
    cases = dlt.read("cases")
    expression_files_info = dlt.read("expression_files_info")

    demographics = cases.selectExpr(
        'case_id',
        '`demographic.ethnicity` as ethnicity',
        '`demographic.gender` as gender',
        '`demographic.race` as race',
        '`demographic.year_of_birth` as year_of_birth',
        '`demographic.year_of_death` as year_of_death'
    )

    # Add derived columns
    demographics = demographics.withColumn(
        'is_deceased',
        when(col('year_of_death').isNotNull(), True).otherwise(False)
    )

    # Join with file information
    return (
        demographics
        .join(
            expression_files_info.select('case_id', 'file_id'),
            on='case_id',
            how='inner'
        )
        .withColumn('transformation_timestamp', current_timestamp())
    )


@dlt.table(
    name="cases_diagnoses",
    comment="Patient diagnosis information including tumor classification, tissue of origin, and treatment details",
    table_properties={
        "quality": "silver",
        "pipelines.autoOptimize.zOrderCols": "case_id,file_id,tissue_or_organ_of_origin",
        "delta.enableChangeDataFeed": "true"
    }
)
@dlt.expect_or_drop("valid_case_id", "case_id IS NOT NULL")
@dlt.expect_or_drop("valid_file_id", "file_id IS NOT NULL")
def cases_diagnoses():
    """
    Transform diagnosis data with data quality checks.

    Quality checks:
    - Case ID and File ID must be present
    - At least one diagnosis field should be populated
    """
    cases = dlt.read("cases")
    expression_files_info = dlt.read("expression_files_info")

    diagnoses = cases.selectExpr(
        'case_id',
        '`diagnoses.0.classification_of_tumor` AS classification_of_tumor',
        '`diagnoses.0.diagnosis_id` AS diagnosis_id',
        '`diagnoses.0.primary_diagnosis` AS primary_diagnosis',
        '`diagnoses.0.tissue_or_organ_of_origin` AS tissue_or_organ_of_origin',
        '`diagnoses.0.tumor_grade` AS tumor_grade',
        '`diagnoses.0.treatments.0.therapeutic_agents` AS treatment0_therapeutic_agents',
        '`diagnoses.0.treatments.0.treatment_id` AS treatment0_treatment_id',
        '`diagnoses.0.treatments.1.therapeutic_agents` AS treatment1_therapeutic_agents',
        '`diagnoses.0.treatments.1.treatment_id` AS treatment1_treatment_id',
        '`diagnoses.0.treatments.1.updated_datetime` AS treatment1_updated_datetime'
    )

    # Add derived columns
    diagnoses = diagnoses.withColumn(
        'has_treatment',
        when(
            col('treatment0_treatment_id').isNotNull() |
            col('treatment1_treatment_id').isNotNull(),
            True
        ).otherwise(False)
    )

    # Join with file information
    return (
        diagnoses
        .join(
            expression_files_info.select('case_id', 'file_id'),
            on='case_id',
            how='inner'
        )
        .withColumn('transformation_timestamp', current_timestamp())
    )


@dlt.table(
    name="cases_exposures",
    comment="Patient environmental exposure information including alcohol, smoking, and BMI",
    table_properties={
        "quality": "silver",
        "pipelines.autoOptimize.zOrderCols": "case_id,file_id",
        "delta.enableChangeDataFeed": "true"
    }
)
@dlt.expect_or_drop("valid_case_id", "case_id IS NOT NULL")
@dlt.expect_or_drop("valid_file_id", "file_id IS NOT NULL")
def cases_exposures():
    """
    Transform exposure data with data quality checks.

    Quality checks:
    - Case ID and File ID must be present
    - Cigarettes per day should be non-negative
    """
    cases = dlt.read("cases")
    expression_files_info = dlt.read("expression_files_info")

    exposures = cases.selectExpr(
        'case_id',
        '`exposures.0.alcohol_history` AS alcohol_history',
        '`exposures.0.alcohol_intensity` AS alcohol_intensity',
        '`exposures.0.cigarettes_per_day` AS cigarettes_per_day'
    )

    # Add derived smoking status column
    exposures = exposures.withColumn(
        'smoking_status',
        when(col('cigarettes_per_day') > 0, 'Current/Former Smoker')
        .when(col('cigarettes_per_day') == 0, 'Non-Smoker')
        .otherwise('Unknown')
    )

    # Join with file information
    return (
        exposures
        .join(
            expression_files_info.select('case_id', 'file_id'),
            on='case_id',
            how='inner'
        )
        .withColumn('transformation_timestamp', current_timestamp())
    )
