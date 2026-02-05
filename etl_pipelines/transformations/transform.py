"""
TCGA Data Transformation DLT Pipeline

This module defines Delta Live Tables for transforming bronze TCGA data into
silver tables with cleaned, validated, and enriched data.

Silver Tables created:
- cases_demographics: Patient demographic information joined with file IDs
- cases_diagnoses: Diagnosis and treatment information
- cases_exposures: Environmental exposure information
"""

import dlt
from pyspark.sql.types import *
from pyspark.sql.functions import col, current_timestamp, year, when

# ============================================================================
# Silver Tables: Cleaned and transformed data
# ============================================================================

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
@dlt.expect("valid_gender", "gender IN ('male', 'female') OR gender IS NULL")
@dlt.expect("valid_year_of_birth", "year_of_birth IS NULL OR (year_of_birth >= 1900 AND year_of_birth <= 2020)")
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
@dlt.expect("has_diagnosis", "primary_diagnosis IS NOT NULL OR diagnosis_id IS NOT NULL")
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
        '`diagnoses.0.tumor_stage` AS tumor_stage',
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
@dlt.expect("valid_cigarettes", "cigarettes_per_day IS NULL OR cigarettes_per_day >= 0")
@dlt.expect("valid_years_smoked", "years_smoked IS NULL OR years_smoked >= 0")
def cases_exposures():
    """
    Transform exposure data with data quality checks.

    Quality checks:
    - Case ID and File ID must be present
    - Cigarettes per day should be non-negative
    - Years smoked should be non-negative
    """
    cases = dlt.read("cases")
    expression_files_info = dlt.read("expression_files_info")

    exposures = cases.selectExpr(
        'case_id',
        '`exposures.0.alcohol_history` AS alcohol_history',
        '`exposures.0.alcohol_intensity` AS alcohol_intensity',
        '`exposures.0.cigarettes_per_day` AS cigarettes_per_day',
        '`exposures.0.years_smoked` AS years_smoked'
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