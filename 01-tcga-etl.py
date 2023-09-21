# Databricks notebook source
# MAGIC %md
# MAGIC ## ETL 
# MAGIC In this notebook we conform the downloaded data into a given data model and publish the resulting tables to Unity Catalog for downstream analysis.
# MAGIC The following diagram summarized the dataflow.
# MAGIC
# MAGIC [![](https://mermaid.ink/img/pako:eNqFk1Fr2zAQx7-K0Ah-SUe2tWz1wyB1lFHoYCxbKdjDKNY5FdiSkeTRUvLde7YjW04f6gdb-v9_p9PJuhdaaAE0povFS6YIkUq6mPRDQiL3CDVEMYn23EK0DNV7biTfV2CjEUerMbLm5jnRlTZd3IfLz9fXbOVDJ-IPPLmJKsvyLXKjjQAzQV-TFT4BV0kFk726_HJ1tQlsC4VWYrabb-tkvd0GjAPj5Ay5WbNP2yQaiGP3wddxscjUwfDmkdz9Hizb7gfhb0LOlHtdtTXYQU3SAs_OklLiWf0bNJbCU2PAWqnVYJAaHBfccU_8CpHG6DAclBgGZ3l3Bf4Y7vOeEvsFw5x5v1ouValHO0yYnyXc3KZC8oPSwXoPHa9tayaKpQJq3e9FFl79cbdLD6Agr-A_VHmQxDruPLVDyvK6qd7hxtL7walScnHxHes9baybMeaPsfewuhFG-iNKm1tf-HzuffYw9_18jGdn8cxnYF1SlLDwuYA10iWtwdRcCuy4vmsy2ndTRmMcCih5W7mM4o1DtG3wRgAT0mlDY2daWFLeOr17VgWNS15Z8NAGf4_h9ahCH_RzaO2-w4-vPW4yHw?type=png)](https://mermaid.live/edit#pako:eNqFk1Fr2zAQx7-K0Ah-SUe2tWz1wyB1lFHoYCxbKdjDKNY5FdiSkeTRUvLde7YjW04f6gdb-v9_p9PJuhdaaAE0povFS6YIkUq6mPRDQiL3CDVEMYn23EK0DNV7biTfV2CjEUerMbLm5jnRlTZd3IfLz9fXbOVDJ-IPPLmJKsvyLXKjjQAzQV-TFT4BV0kFk726_HJ1tQlsC4VWYrabb-tkvd0GjAPj5Ay5WbNP2yQaiGP3wddxscjUwfDmkdz9Hizb7gfhb0LOlHtdtTXYQU3SAs_OklLiWf0bNJbCU2PAWqnVYJAaHBfccU_8CpHG6DAclBgGZ3l3Bf4Y7vOeEvsFw5x5v1ouValHO0yYnyXc3KZC8oPSwXoPHa9tayaKpQJq3e9FFl79cbdLD6Agr-A_VHmQxDruPLVDyvK6qd7hxtL7walScnHxHes9baybMeaPsfewuhFG-iNKm1tf-HzuffYw9_18jGdn8cxnYF1SlLDwuYA10iWtwdRcCuy4vmsy2ndTRmMcCih5W7mM4o1DtG3wRgAT0mlDY2daWFLeOr17VgWNS15Z8NAGf4_h9ahCH_RzaO2-w4-vPW4yHw)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 0. Configuration

# COMMAND ----------

# MAGIC %run ./util/configurations

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Define ETL functions

# COMMAND ----------

# DBTITLE 1,expression profiles
def read_expression_files_info(STAGING_PATH):
  return (
    spark.read.csv(f'{STAGING_PATH}/expressions_info.tsv', sep='\t', header=True,inferSchema=True)
    .withColumnRenamed('cases.0.case_id','case_id')
  )

# COMMAND ----------

# DBTITLE 1,cases
def read_cases(STAGING_PATH):
  return spark.read.csv(f'{STAGING_PATH}/cases.tsv', sep='\t', header=True, inferSchema=True)


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
 
def read_expression_profiles(STAGING_PATH):
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
    spark.read.csv(f'{STAGING_PATH}/expressions', comment="#", sep="\t", schema=schema)
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
# MAGIC ## 2. Add Tables

# COMMAND ----------

# DBTITLE 1,add cases
read_cases(STAGING_PATH).write.mode("overwrite").saveAsTable(f'{CATALOG_NAME}.{SCHEMA_NAME}.cases')

# COMMAND ----------

# DBTITLE 1,add expression profile information
read_expression_files_info(STAGING_PATH).write.mode("overwrite").saveAsTable(f'{CATALOG_NAME}.{SCHEMA_NAME}.expression_files_info')

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
expression_profiles_df = read_expression_profiles(STAGING_PATH)
expression_profiles_df.write.mode("overwrite").saveAsTable(f'{CATALOG_NAME}.{SCHEMA_NAME}.expression_profiles')

# COMMAND ----------

# DBTITLE 1,add sample level stats
get_sample_level_expression_stats(expression_profiles_df).write.mode("overwrite").saveAsTable(f'{CATALOG_NAME}.{SCHEMA_NAME}.sample_level_expression_stats')

# COMMAND ----------

# DBTITLE 1,add gene-level stats
get_gene_level_expression_stats(expression_profiles_df).write.mode("overwrite").saveAsTable(f'{CATALOG_NAME}.{SCHEMA_NAME}.gene_level_expression_stats')

# COMMAND ----------

# DBTITLE 1,grant access to tables in the schema 
#grant access to tables in the schema 
my_schema = f'{CATALOG_NAME}.{SCHEMA_NAME}'
for t in [m.tableName for m in sql(f'show tables in {my_schema}').collect()]:
  sql(f"""
  GRANT SELECT, MODIFY
  ON TABLE {my_schema}.{t}
  TO `account users`""")
