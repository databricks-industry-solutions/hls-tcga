# Databricks notebook source
# MAGIC %md
# MAGIC # Download Data from Genomic Data Common (GDC)
# MAGIC
# MAGIC In this notebook, we will:
# MAGIC
# MAGIC - Download clinical information from GDC.
# MAGIC - Fetch the corresponding gene expression profiles.
# MAGIC - Persist the files in a specified cloud path for future access.
# MAGIC
# MAGIC [![](https://mermaid.ink/img/pako:eNplkk1rwzAMhv-K8Ai5dNBtLVtzGDRfZYdBYWWXegc3lleDYwfbYS2l_31OQteU-WDL0vNiWdKJVIYjSUgUnagGkFr6BHoTIPZ7rDFOIN4xh_Fk7P1kVrKdQhf_4SHUWFkze8yMMrbT3c0eF4tiepFeiQ0e_JUSQvxHUmM52iv0nE3DGnFKaryGp7On-TwfhR1WRvObbF6W2bIsR4xH6-UNki6LhzKLB-LcHWE7RxHV35Y1e9ikVKfb3PxoZRiHLCQhK6aAUv2mhbE189JoENbUsMqzL6qzbYm-2sMKNUJxaCw61yFBsbZGyFDDjoL7-1fIt2u0TjoPZecP7YBQbaiUaXmA0gGimkxIjeEtyUPr-vJT0reFkiSYHAVrlackpB7QtuHMY8GlN5Yk3rY4Iaz15uOoq8t9YHLJwjdrkgimXPBir3kfRqSflPMvKLewvw?type=png)](https://mermaid.live/edit#pako:eNplkk1rwzAMhv-K8Ai5dNBtLVtzGDRfZYdBYWWXegc3lleDYwfbYS2l_31OQteU-WDL0vNiWdKJVIYjSUgUnagGkFr6BHoTIPZ7rDFOIN4xh_Fk7P1kVrKdQhf_4SHUWFkze8yMMrbT3c0eF4tiepFeiQ0e_JUSQvxHUmM52iv0nE3DGnFKaryGp7On-TwfhR1WRvObbF6W2bIsR4xH6-UNki6LhzKLB-LcHWE7RxHV35Y1e9ikVKfb3PxoZRiHLCQhK6aAUv2mhbE189JoENbUsMqzL6qzbYm-2sMKNUJxaCw61yFBsbZGyFDDjoL7-1fIt2u0TjoPZecP7YBQbaiUaXmA0gGimkxIjeEtyUPr-vJT0reFkiSYHAVrlackpB7QtuHMY8GlN5Yk3rY4Iaz15uOoq8t9YHLJwjdrkgimXPBir3kfRqSflPMvKLewvw)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 0. Initial Configurations

# COMMAND ----------

# DBTITLE 1,making function definitions accessible
# MAGIC %run ./util/functions

# COMMAND ----------

# DBTITLE 1,creating notebook configuations
# MAGIC %run ./util/notebook-config

# COMMAND ----------

# DBTITLE 1,load configurations 
import json
with open('./util/configs.json', 'r') as f:
    configs = json.load(f)

staging_path=configs['paths']['staging_path']
expression_files_path = configs['paths']['expression_files_path']

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Download Data

# COMMAND ----------

# DBTITLE 1,download files list
import pandas as pd
import os

file_fields = [
  "access",
  "data_format",
  "data_type",
  "file_id",
  "cases.project.project_id",
  "data_category",
  "experimental_strategy",
  "file_id",
  "file_name",
  "file_size",
  "type",
  "cases.case_id"
]

files_filters = [
  ('data_format','in',['TSV']),
  ('data_type','in',['Gene Expression Quantification']),
  ('cases.project.program.name','in',['TCGA']),
  ('access','in',['open'])
  ]

path = f"/dbfs{staging_path}/expressions_info.tsv"
download_table(files_endpt,file_fields,path,size=20000,filters=files_filters)


files_list_pdf=pd.read_csv(path,sep='\t')
print(f'downloaded {files_list_pdf.shape[0]} records')
files_list_pdf.head()

# COMMAND ----------

# MAGIC %md
# MAGIC Now we use the list of the files to download expression profiles. Note that we are downloading ~11000 files and this can take some time (even with 32 cores concurrently). 

# COMMAND ----------

# DBTITLE 1,download expressions
uuids=files_list_pdf.file_id.to_list()
download_expressions(f"/dbfs{expression_files_path}",uuids,n_workers=48)

# COMMAND ----------

# DBTITLE 1,take a look at a sample expression file
sample_file=dbutils.fs.ls(f"{expression_files_path}")[0].path
df=spark.read.csv(sample_file,sep='\t',comment="#",header=True)
print(f"n_records in {sample_file} is {df.count()}")
display(df)

# COMMAND ----------

# DBTITLE 1,download cases
demographic_fields = [
  "demographic.ethnicity",
  "demographic.gender",
  "demographic.race",
  "demographic.year_of_birth",
  "demographic.year_of_death"
]

diagnoses_fields = [
  "diagnoses.classification_of_tumor",
  "diagnoses.diagnosis_id",
  "diagnoses.primary_diagnosis",
  "diagnoses.tissue_or_organ_of_origin",
  "diagnoses.tumor_grade",
  "diagnoses.tumor_stage",
  "diagnoses.treatments.therapeutic_agents",
  "diagnoses.treatments.treatment_id",
  "diagnoses.treatments.updated_datetime"
]

exposures_fields = [
"exposures.alcohol_history",
"exposures.alcohol_intensity",
"exposures.bmi",
"exposures.cigarettes_per_day",
"exposures.height",
"exposures.updated_datetime",
"exposures.weight",
"exposures.years_smoked"]

fields = ['case_id']+demographic_fields+diagnoses_fields+exposures_fields

cases_filters = [
  ('cases.project.program.name','in',['TCGA']),
]

download_table(cases_endpt,fields,f"/dbfs{staging_path}/cases.tsv",size=100000,filters=cases_filters)

df=spark.read.csv(f"{staging_path}/cases.tsv",sep='\t',header=True)
print(f"n_records in cases is {df.count()}")
display(df)
