# Databricks notebook source
# MAGIC %md
# MAGIC ## Delta Live Table (DLT)
# MAGIC In this notebook we show how you can preform the tasks in [01-tcga-etl]("$./01-tcga-etl") using [Delta Live Tables](https://www.databricks.com/product/delta-live-tables)

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *
import dlt

import json
with open('./util/configs.json', 'r') as f:
    configs = json.load(f)
catalog_name = configs['catalog']['ctalog_name']
schema_name = configs['catalog']['schema_name']
staging_path = configs['paths']['staging_path']
expression_files_path = configs['paths']['expression_files_path']


# COMMAND ----------

# DBTITLE 1,create a view of all cases
@dlt.create_view(
  comment=f"information related to the expression files downloaded from GDC saved on {expression_files_path}."
)
def expression_files_info():
  return (
    spark.read.csv(f'{staging_path}/expressions_info.tsv', sep='\t', header=True)
    .withColumnRenamed('cases.0.case_id','case_id')
  )

# COMMAND ----------

# 
# TCGA Dataset
# Data Set Information
# ====================
# * cases : demographic, exposures and diagnoses fields: {staging_path}/cases.tsv
# * expressions 
#


@dlt.create_view(
  comment="Patient demographics, diagnoses and exposures ingested from staging_path."
)
def cases():
  return spark.read.csv(f'{staging_path}/cases.tsv', sep='\t', header=True)


# COMMAND ----------

@dlt.create_table(
  comment="patient demographics ingested from files downloaded from GDC API.",
  table_properties={
    "pipelines.autoOptimize.managed": "true",
    "myCompanyPipeline.quality": "bronze",
  }
)
def cases_demographic():
  df = dlt.read("cases").selectExpr(
    'case_id',
    '`demographic.ethnicity` as ethnicity',
    '`demographic.gender` as gender',
    '`demographic.race` as race',
    '`demographic.year_of_birth` as year_of_birth',
    '`demographic.year_of_death` as year_of_death'
  )
  return df.join(dlt.read("expression_files_info").select('case_id','file_id'), on = 'case_id')

# COMMAND ----------

@dlt.create_table(
  comment="patient diagnoses ingested from files downloaded from GDC API.",
  table_properties={
    "pipelines.autoOptimize.managed": "true",
    "myCompanyPipeline.quality": "bronze",

  }
)
def cases_diagnoses():
  df = dlt.read("cases").selectExpr(
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
  return df.join(dlt.read("expression_files_info").select('case_id','file_id'), on = 'case_id')

# COMMAND ----------

@dlt.create_table(
  comment="patient exposures ingested from files downloaded from GDC API.",
  table_properties={
    "pipelines.autoOptimize.managed": "true",
    "myCompanyPipeline.quality": "bronze",
  }
)

def cases_exposures():
  df = dlt.read("cases").selectExpr(
    'case_id',
    '`exposures.0.alcohol_history` AS alcohol_history',
    '`exposures.0.alcohol_intensity` AS  alcohol_intensity',
    '`exposures.0.cigarettes_per_day` AS cigarettes_per_day',
    '`exposures.0.years_smoked` AS years_smoked',
  )
  return df.join(dlt.read("expression_files_info").select('case_id','file_id'), on = 'case_id')

# COMMAND ----------

@dlt.create_table(
  comment="expression profiles ingested from files downloaded from GDC API.",
  table_properties={
    "pipelines.autoOptimize.managed": "true",
    "myCompanyPipeline.quality": "bronze",

  }
)
def expression_profiles_raw():
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
      spark.readStream.format("cloudFiles")
      .option("cloudFiles.format", "csv")
      .option("comment", "#")
      .option("sep", "\t")
      .option("rescuedDataColumn", "_rescued_data") # makes sure that you don't lose data
      .schema(schema) # provide a schema here for the files
      .load(f'{staging_path}/expressions')
      .withColumn('file_id', substring_index(input_file_name(),'/',-1))
    )

  return df

# COMMAND ----------

@dlt.create_table(
  comment="expression profiles filtered to have the correct number of genes.",
  table_properties={
    "pipelines.autoOptimize.managed": "true",
    "myCompanyPipeline.quality": "silver",
  }
)

def expression_profiles_validated():
  n_features=60660
  return(
    dlt.read("expression_profiles_raw")
    .filter(~col('gene_id').rlike('N_'))
    # .selectExpr('*','count(gene_id) over (partition by file_id) as total_counts')
    # .filter(f'total_counts =={n_features}')
  )


# COMMAND ----------

@dlt.create_table(
  comment="sample-level expression stats: mean and variance for counts",
  table_properties={
    "pipelines.autoOptimize.managed": "true",
    "myCompanyPipeline.quality": "silver",
  }
)

def sample_level_expression_stats():
    return(
      dlt.read("expression_profiles_validated")
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


@dlt.create_table(
  comment="gene-level expression stats: mean and variance for counts",
  table_properties={
    "pipelines.autoOptimize.managed": "true",
    "myCompanyPipeline.quality": "silver",
  }
)

def gene_level_expression_stats():
    return(
      dlt.read('expression_profiles_validated')
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


