# Databricks notebook source
# MAGIC %md
# MAGIC ## ETL 
# MAGIC In this notebook we conform the raw data into a data model and publish the data to Unity Catalog for downstream analysis.
# MAGIC The following summarized the dataflow
# MAGIC
# MAGIC [![](https://mermaid.ink/img/pako:eNqFk1Fr2zAQx7-K0Ah-SUa2tmz1wyB1lFJoYSzbKNjDKNY5FdiSkeTSUvLdd7ajWE4f6gdbuv_v9NfJujdaaAE0prPZW6YIkUq6mPRDQiL3BDVEMYl23EI0D6N_uZF8V4GNTjhKjZE1N6-JrrTp8j5dfr2-ZkufOhK_4cWNVFmW75EbbQSYEfqWLPEJuEoqGOXl5cXV1TqQLRRaicluvq-S1WYTMA6MkxPkZsW-bJJoIA7dB1-H2SxTe8ObJ3L_a5BsuxsCRaVbQazThu9h0JK0wPOypJR4Pv-GGEvhpTFgrdRqEEgNjgvuuCd-hkhjdJgOSpz5_klIpo5-R0O_UOiV96vkUpX6JIdG-ZnR-i4Vku-VDtZ77HhtWzNSLBVQ634nsvDR2_ttugcFeQXPUOWBiXXceWqLlOV1U33A9SUfCySLxQ8s87ifbsaYP7Vew6LG0yCLzxha3_l6p3Ovs8ep7uenfHaWz7wD60wxhPVOA1gandMaTM2lwKbqGyOjfcNkNMahgJK3lcsoXipE2wYvADAh8frQ2JkW5pS3Tm9fVUHjklcWPLTGv2J4fYpCn_QwdG_fxIf_Lgop1Q?type=png)](https://mermaid.live/edit#pako:eNqFk1Fr2zAQx7-K0Ah-SUa2tmz1wyB1lFJoYSzbKNjDKNY5FdiSkeTSUvLdd7ajWE4f6gdbuv_v9NfJujdaaAE0prPZW6YIkUq6mPRDQiL3BDVEMYl23EI0D6N_uZF8V4GNTjhKjZE1N6-JrrTp8j5dfr2-ZkufOhK_4cWNVFmW75EbbQSYEfqWLPEJuEoqGOXl5cXV1TqQLRRaicluvq-S1WYTMA6MkxPkZsW-bJJoIA7dB1-H2SxTe8ObJ3L_a5BsuxsCRaVbQazThu9h0JK0wPOypJR4Pv-GGEvhpTFgrdRqEEgNjgvuuCd-hkhjdJgOSpz5_klIpo5-R0O_UOiV96vkUpX6JIdG-ZnR-i4Vku-VDtZ77HhtWzNSLBVQ634nsvDR2_ttugcFeQXPUOWBiXXceWqLlOV1U33A9SUfCySLxQ8s87ifbsaYP7Vew6LG0yCLzxha3_l6p3Ovs8ep7uenfHaWz7wD60wxhPVOA1gandMaTM2lwKbqGyOjfcNkNMahgJK3lcsoXipE2wYvADAh8frQ2JkW5pS3Tm9fVUHjklcWPLTGv2J4fYpCn_QwdG_fxIf_Lgop1Q)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 0. Configuration

# COMMAND ----------

# MAGIC %run ./util/notebook-config 

# COMMAND ----------

import json
with open('./util/configs.json', 'r') as f:
    configs = json.load(f)
catalog_name = configs['catalog']['ctalog_name']
schema_name = configs['catalog']['schema_name']
staging_path = configs['paths']['staging_path']
expression_files_path = configs['paths']['expression_files_path']


# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Define ETL functions

# COMMAND ----------

# DBTITLE 1,load expression profiles
def read_expression_files_info(staging_path):
  return (
    spark.read.csv(f'{staging_path}/expressions_info.tsv', sep='\t', header=True,inferSchema=True)
    .withColumnRenamed('cases.0.case_id','case_id')
  )

# COMMAND ----------

# DBTITLE 1,Cases
# Patient demographics, diagnoses and exposures ingested from staging_path

def read_cases(staging_path):
  return spark.read.csv(f'{staging_path}/cases.tsv', sep='\t', header=True, inferSchema=True)


# COMMAND ----------

# DBTITLE 1,Demographics 
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
# MAGIC ## 2. Create Catalog

# COMMAND ----------

# Create a catalog.
spark.sql(f"CREATE CATALOG IF NOT EXISTS {catalog_name}")

# COMMAND ----------

# Grant create and use catalog permissions for the catalog to all users on the account.
# This also works for other account-level groups and individual users.
spark.sql(f"""
  GRANT CREATE, USE CATALOG
  ON CATALOG {catalog_name}
  TO `account users`""")

# COMMAND ----------

# Create a schema in the catalog that was set earlier.
spark.sql(f"""
  CREATE SCHEMA IF NOT EXISTS {catalog_name}.{schema_name}
  COMMENT 'schmea for TCGA expression profiles and metadata'""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Add Tables

# COMMAND ----------

# DBTITLE 1,add cases
read_cases(staging_path).write.mode("overwrite").saveAsTable(f'{catalog_name}.{schema_name}.cases')

# COMMAND ----------

# DBTITLE 1,add expression profile information
read_expression_files_info(staging_path).write.mode("overwrite").saveAsTable(f'{catalog_name}.{schema_name}.expression_files_info')

# COMMAND ----------

cases_df = sql(f'SELECT * from {catalog_name}.{schema_name}.cases')
expression_files_info_df = sql(f'SELECT * from {catalog_name}.{schema_name}.expression_files_info')

# COMMAND ----------

# DBTITLE 1,add demographics
extract_cases_demographics(cases_df,expression_files_info_df).write.mode("overwrite").saveAsTable(f'{catalog_name}.{schema_name}.demographics')

# COMMAND ----------

# DBTITLE 1,add diagnosis
extract_cases_diagnoses(cases_df,expression_files_info_df).write.mode("overwrite").saveAsTable(f'{catalog_name}.{schema_name}.diagnoses')

# COMMAND ----------

# DBTITLE 1,add exposures
extract_cases_exposures(cases_df,expression_files_info_df).write.mode("overwrite").saveAsTable(f'{catalog_name}.{schema_name}.exposures')

# COMMAND ----------

# DBTITLE 1,add expression profiles
expression_profiles_df = read_expression_profiles(staging_path)
expression_profiles_df.write.mode("overwrite").saveAsTable(f'{catalog_name}.{schema_name}.expression_profiles')

# COMMAND ----------

# DBTITLE 1,add sample level stats
get_sample_level_expression_stats(expression_profiles_df).write.mode("overwrite").saveAsTable(f'{catalog_name}.{schema_name}.sample_level_expression_stats')

# COMMAND ----------

# DBTITLE 1,add gene-level stats
get_gene_level_expression_stats(expression_profiles_df).write.mode("overwrite").saveAsTable(f'{catalog_name}.{schema_name}.gene_level_expression_stats')