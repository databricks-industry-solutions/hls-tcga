import dlt
from pyspark.sql.types import *
from pyspark.sql.functions import col

import os
import json

# Determine the absolute path to the JSON file
config_path = os.path.abspath('../../config.json')

# Load configurations from the JSON file
with open(config_path, 'r') as file:
    config = json.load(file)

# Access the configurations
catalog = config['lakehouse']['catalog']
schema = config['lakehouse']['schema']
volume = config['lakehouse']['volume']
# entity_csv_schema = spark.conf.get("mypipeline.entity_csv_schema")
volume_path = f'/Volumes/{catalog}/{schema}/{volume}'

@dlt.table
def expression_files_info():
  return (
    spark.read.csv(f'{volume_path}/expressions_info.tsv', sep='\t', header=True,inferSchema=True)
    .withColumnRenamed('cases.0.case_id','case_id')
  )

@dlt.table
def cases():
  return spark.read.csv(f'{volume_path}/cases.tsv', sep='\t', header=True, inferSchema=True)

@dlt.table
def expression_profiles():
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
    spark.read.csv(f'{volume_path}/expressions', comment="#", sep="\t", schema=schema)
    .selectExpr('*','_metadata.file_name as file_id')
    .filter(~col('gene_id').rlike('N_') & ~col('gene_id').contains('gene_id'))
  )
    
  return df

