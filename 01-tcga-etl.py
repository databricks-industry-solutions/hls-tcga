# Databricks notebook source
# MAGIC %md
# MAGIC ## ETL 
# MAGIC In this notebook we conform the downloaded data into a given data model and publish the resulting tables to Unity Catalog for downstream analysis.
# MAGIC The following diagram summarized the dataflow.
# MAGIC
# MAGIC [![](https://mermaid.ink/img/pako:eNqFk19r2zAUxb-K0Ah-SUbWP6z1wyB1lFHoYCzdKNjDKNZ1KrAlI8lbS8l377UdxXL2sDwk1jm_q6Or-L7RQgugMZ3N3jJFiFTSxaR_JCRyz1BDFJNoxy1E81D9xY3kuwpsdMLRaoysuXlNdKVNV_fh6uL2li196Ug8wosbqbIs_0XutBFgRuhzssRPwFVSwWgvry6vr9eBbaHQSkxOc7NKVptNwDgwTk6QuxX7tEmigTh0P_h1mM0ytTe8eSYPPwbLtrtBMPwvKSVexKAnaYF3ZQfp96CxFF4aA9ZKrQaD1OC44I574nuINEaH5aDEWebPhGTqmHcM9BuFWXm_Sy5VqU92GJSfBa3vUyH5Xulgv6eO17Y1I8VSAbXuTyILr3592KZ7UJBX8AeqPAixjjtPbZGyvG6q_3B9y8cGyWLxBds8nqdbMeZvrfewqfE2yOIjSut73-907X32NPX9-lTPzuqZT2BdKErY71TA1uic1mBqLgUOVD8UGe2HJaMxPgooeVu5jOILhWjb4AsATEinDY2daWFOeev09lUVNC55ZcFDa_xXDK9PKvRF34bJ7Qf48A50ECgm?type=png)](https://mermaid.live/edit#pako:eNqFk19r2zAUxb-K0Ah-SUbWP6z1wyB1lFHoYCzdKNjDKNZ1KrAlI8lbS8l377UdxXL2sDwk1jm_q6Or-L7RQgugMZ3N3jJFiFTSxaR_JCRyz1BDFJNoxy1E81D9xY3kuwpsdMLRaoysuXlNdKVNV_fh6uL2li196Ug8wosbqbIs_0XutBFgRuhzssRPwFVSwWgvry6vr9eBbaHQSkxOc7NKVptNwDgwTk6QuxX7tEmigTh0P_h1mM0ytTe8eSYPPwbLtrtBMPwvKSVexKAnaYF3ZQfp96CxFF4aA9ZKrQaD1OC44I574nuINEaH5aDEWebPhGTqmHcM9BuFWXm_Sy5VqU92GJSfBa3vUyH5Xulgv6eO17Y1I8VSAbXuTyILr3592KZ7UJBX8AeqPAixjjtPbZGyvG6q_3B9y8cGyWLxBds8nqdbMeZvrfewqfE2yOIjSut73-907X32NPX9-lTPzuqZT2BdKErY71TA1uic1mBqLgUOVD8UGe2HJaMxPgooeVu5jOILhWjb4AsATEinDY2daWFOeev09lUVNC55ZcFDa_xXDK9PKvRF34bJ7Qf48A50ECgm)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 0. Configuration

# COMMAND ----------

# MAGIC %run ./util/configurations

# COMMAND ----------

# DBTITLE 1,setup paths 
staging_path = f"/home/{USER}/{CATALOG_NAME}/{SCHEMA_NAME}/staging"
expression_files_path = f"{staging_path}/expressions"

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Define ETL functions

# COMMAND ----------

# DBTITLE 1,expression profiles
def read_expression_files_info(staging_path):
  return (
    spark.read.csv(f'{staging_path}/expressions_info.tsv', sep='\t', header=True,inferSchema=True)
    .withColumnRenamed('cases.0.case_id','case_id')
  )

# COMMAND ----------

# DBTITLE 1,cases
def read_cases(staging_path):
  return spark.read.csv(f'{staging_path}/cases.tsv', sep='\t', header=True, inferSchema=True)


# COMMAND ----------

# DBTITLE 1,demographics 
## patient demographics ingested from files downloaded from GDC API.
  
def extract_cases_demographics(cases_df,expression_files_info_df):
  df = cases_df.selectExpr(
    'case_id',
    '`demographic.ethnicity` as ethnicity',
    '`demographic.gender` as gender',
    '`demographic.race` as race',
    '`demographic.year_of_birth` as year_of_birth',
    '`demographic.year_of_death` as year_of_death'
  )
  return df.join(expression_files_info_df.select('case_id','file_id'), on = 'case_id')

# COMMAND ----------

# DBTITLE 1,diagnoses
#patient diagnoses ingested from files downloaded from GDC API.

def extract_cases_diagnoses(cases_df,expression_files_info_df):
  df = cases_df.selectExpr(
    'case_id',
    '`diagnoses.0.classification_of_tumor` AS classification_of_tumor',
    '`diagnoses.0.diagnosis_id` AS diagnosis_id',
    '`diagnoses.0.primary_diagnosis` AS primary_diagnosis',
    '`diagnoses.0.tissue_or_organ_of_origin` AS tissue_or_organ_of_origin',
    '`diagnoses.0.treatments.0.therapeutic_agents` AS treatments0_therapeutic_agents',
    '`diagnoses.0.treatments.0.treatment_id` AS treatments0_treatment_id',
    '`diagnoses.0.treatments.1.therapeutic_agents` AS treatments1_therapeutic_agents',
    '`diagnoses.0.treatments.1.treatment_id` AS treatments1_treatment_id',
    '`diagnoses.0.treatments.1.updated_datetime` AS treatments1_updated_datetime',
    '`diagnoses.0.tumor_grade` AS tumor_grade'
  )
  return df.join(expression_files_info_df.select('case_id','file_id'), on = 'case_id')

# COMMAND ----------

# DBTITLE 1,exposures
#"patient exposures ingested from files downloaded from GDC API.",

def extract_cases_exposures(cases_df,expression_files_info_df):
  df = cases_df.selectExpr(
    'case_id',
    '`exposures.0.alcohol_history` AS alcohol_history',
    '`exposures.0.alcohol_intensity` AS  alcohol_intensity',
    '`exposures.0.cigarettes_per_day` AS cigarettes_per_day',
    '`exposures.0.years_smoked` AS years_smoked',
  )
  return df.join(expression_files_info_df.select('case_id','file_id'), on = 'case_id')

# COMMAND ----------

# DBTITLE 1,expression_profiles
#"expression profiles ingested from files downloaded from GDC API.",
 
def read_expression_profiles(staging_path):
  schema = StructType([
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

  df = (
    spark.read.csv(f'{staging_path}/expressions', comment="#", sep="\t", schema=schema)
    .selectExpr('*','_metadata.file_name as file_id')
    # .withColumn('file_id', substring_index('_metadata.file_path','/',-1))
    .filter(~col('gene_id').rlike('N_'))
    )
    
  return df

# COMMAND ----------

# DBTITLE 1,sample-level stats
#sample-level expression stats: mean and variance for counts
  
def get_sample_level_expression_stats(expression_profiles_df):
    return(
      expression_profiles_df
      .groupBy('file_id')
      .agg(
          mean(col('fpkm_unstranded')).alias('m_fpkm_unstranded'),
          variance(col('fpkm_unstranded')).alias('v_fpkm'),
          mean(col('fpkm_uq_unstranded')).alias('m_uq_fpkm'),
          variance(col('fpkm_uq_unstranded')).alias('v_uq_fpkm'),
          mean(col('tpm_unstranded')).alias('m_tpm'),
          variance(col('tpm_unstranded')).alias('v_tpm'),
      )
    )

# COMMAND ----------

# DBTITLE 1,gene-level stats
#gene-level expression stats: mean and variance for counts"

def get_gene_level_expression_stats(expression_profiles_df):
    return(
      expression_profiles_df
      .groupBy('gene_id')
      .agg(
          mean(col('fpkm_unstranded')).alias('m_fpkm'),
          variance(col('fpkm_unstranded')).alias('v_fpkm'),
          mean(col('fpkm_uq_unstranded')).alias('m_uq_fpkm'),
          variance(col('fpkm_uq_unstranded')).alias('v_uq_fpkm'),
          mean(col('tpm_unstranded')).alias('m_tpm'),
          variance(col('tpm_unstranded')).alias('v_tpm'),
      )
  )

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Create UC Catalog

# COMMAND ----------

# Create a catalog.
spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG_NAME}")

# COMMAND ----------

# Grant create and use catalog permissions for the catalog to all users on the account.
# This also works for other account-level groups and individual users.
spark.sql(f"""
  GRANT CREATE, USE CATALOG
  ON CATALOG {CATALOG_NAME}
  TO `account users`""")

# COMMAND ----------

# Create a schema in the catalog that was set earlier.
spark.sql(f"""
  CREATE SCHEMA IF NOT EXISTS {CATALOG_NAME}.{SCHEMA_NAME}
  COMMENT 'schmea for TCGA expression profiles and metadata'""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Add Tables

# COMMAND ----------

# DBTITLE 1,add cases
read_cases(staging_path).write.mode("overwrite").saveAsTable(f'{CATALOG_NAME}.{SCHEMA_NAME}.cases')

# COMMAND ----------

# DBTITLE 1,add expression profile information
read_expression_files_info(staging_path).write.mode("overwrite").saveAsTable(f'{CATALOG_NAME}.{SCHEMA_NAME}.expression_files_info')

# COMMAND ----------

cases_df = sql(f'SELECT * from {CATALOG_NAME}.{SCHEMA_NAME}.cases')
expression_files_info_df = sql(f'SELECT * from {CATALOG_NAME}.{SCHEMA_NAME}.expression_files_info')

# COMMAND ----------

# DBTITLE 1,add demographics
extract_cases_demographics(cases_df,expression_files_info_df).write.mode("overwrite").saveAsTable(f'{CATALOG_NAME}.{SCHEMA_NAME}.demographics')

# COMMAND ----------

# DBTITLE 1,add diagnosis
extract_cases_diagnoses(cases_df,expression_files_info_df).write.mode("overwrite").saveAsTable(f'{CATALOG_NAME}.{SCHEMA_NAME}.diagnoses')

# COMMAND ----------

# DBTITLE 1,add exposures
extract_cases_exposures(cases_df,expression_files_info_df).write.mode("overwrite").saveAsTable(f'{CATALOG_NAME}.{SCHEMA_NAME}.exposures')

# COMMAND ----------

# DBTITLE 1,add expression profiles
expression_profiles_df = read_expression_profiles(staging_path)
expression_profiles_df.write.mode("overwrite").saveAsTable(f'{CATALOG_NAME}.{SCHEMA_NAME}.expression_profiles')

# COMMAND ----------

# DBTITLE 1,add sample level stats
get_sample_level_expression_stats(expression_profiles_df).write.mode("overwrite").saveAsTable(f'{CATALOG_NAME}.{SCHEMA_NAME}.sample_level_expression_stats')

# COMMAND ----------

# DBTITLE 1,add gene-level stats
get_gene_level_expression_stats(expression_profiles_df).write.mode("overwrite").saveAsTable(f'{CATALOG_NAME}.{SCHEMA_NAME}.gene_level_expression_stats')
