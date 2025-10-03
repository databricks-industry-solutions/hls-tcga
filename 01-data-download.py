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
# MAGIC [![](https://mermaid.ink/img/pako:eNplkk1vozAQhv-KNauIC434DMGHlQKEaA8rRdqqh4YeXBgSS2AjY7TJRvnva4jSpK0Ptmfmee2xZ85QygqBwmx2LgQhXHBNybQlxNIHbNGixHpnPVr2o_eFKc7eG-ytD9yEOsVbpk6pbKQadT8CL47Xzk16J57xqO9UXdffkUSqCtUdilLHjAeu4QLvYSfwwzB7CPdYSlF9yma5Sld5_sBoVJp_QpLV2s1T60pcxsVMl9msEHvFugN5TgqR7DL5VzSSVSQ1SfCSNeSXqKVqmeZSkFrJlmyy9K0Q6S5HXR7IBgWS9bFT2PcjslWy5ub3RoQ8Pf0k2W6Lque9JvnoN4UgjLzIZmjRMMmVKQTYsFe8AqrVgDa0aO4cTZhqUMBUmwKo2VZYs6HRBZj8jaxj4lXK9qZUctgfgNas6Y01dBXTmHFmHtl-8a4rrqW66dig5Z-TKG82TtHf1yaaesmcjWKq3CA00IU73Q30DEegfujNXScMY88LI9ePYxtOQGN3HnhRFEROsPSWkeNdbPg3JevMIydeRIvYc32zBKF_-Q956dPG?type=png)](https://mermaid.live/edit#pako:eNplkk1vozAQhv-KNauIC434DMGHlQKEaA8rRdqqh4YeXBgSS2AjY7TJRvnva4jSpK0Ptmfmee2xZ85QygqBwmx2LgQhXHBNybQlxNIHbNGixHpnPVr2o_eFKc7eG-ytD9yEOsVbpk6pbKQadT8CL47Xzk16J57xqO9UXdffkUSqCtUdilLHjAeu4QLvYSfwwzB7CPdYSlF9yma5Sld5_sBoVJp_QpLV2s1T60pcxsVMl9msEHvFugN5TgqR7DL5VzSSVSQ1SfCSNeSXqKVqmeZSkFrJlmyy9K0Q6S5HXR7IBgWS9bFT2PcjslWy5ub3RoQ8Pf0k2W6Lque9JvnoN4UgjLzIZmjRMMmVKQTYsFe8AqrVgDa0aO4cTZhqUMBUmwKo2VZYs6HRBZj8jaxj4lXK9qZUctgfgNas6Y01dBXTmHFmHtl-8a4rrqW66dig5Z-TKG82TtHf1yaaesmcjWKq3CA00IU73Q30DEegfujNXScMY88LI9ePYxtOQGN3HnhRFEROsPSWkeNdbPg3JevMIydeRIvYc32zBKF_-Q956dPG)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 0. Initial configurations

# COMMAND ----------

# MAGIC %run ./00-setup

# COMMAND ----------

FORCE_DOWNLOAD=False

# COMMAND ----------

# MAGIC %md
# MAGIC ## Define Functions

# COMMAND ----------

def download_table(end_point, fields, output_file, size=100000, filters=None):
        resource = end_point.split('/')[-1]
        
        if resource == 'files':
            if filters:
                _filters = [{'op': f"{m[1]}", 'content': {'field': f"{m[0]}", 'value': m[2]}} for m in filters]
                api_filter = {
                    "op": "and",
                    "content": _filters
                }
            else:
                api_filter = None
        else:
            if filters:
                _filters = [{'op': f"{m[1]}", 'content': {'field': f"{m[0]}", 'value': m[2]}} for m in filters]
                api_filter = {
                    "op": "and",
                    "content": _filters
                }
            else:
                api_filter = None

        fields = ','.join(fields)
        params = {
            "filters": api_filter,
            "fields": fields,
            "format": "TSV",
            "size": size
        }
        
        response = requests.post(end_point, json=params)
        with open(output_file, 'w') as f:
            f.write(response.content.decode("utf-8"))

# COMMAND ----------

from concurrent.futures import ThreadPoolExecutor
def download_single_expression(
    uuid,
    target_directory_path
):
    local_filename = f"{target_directory_path}/{uuid}"
    url = f'{data_endpt}{uuid}'
    with open(local_filename, 'w') as f:
        f.write(
            requests.get(url).content.decode("utf-8")
        )

def download_expressions(
    target_directory_path,
    uuids,
    n_workers=64
):
    params = [
        (uid, target_directory_path)
        for uid in uuids
    ]
    with ThreadPoolExecutor(
        max_workers=n_workers
    ) as executor:
        executor.map(
            lambda pair: download_single_expression(*pair),
            params
        )

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Download data from GDC

# COMMAND ----------

FORCE_DOWNLOAD=False

# COMMAND ----------

# DBTITLE 1,download files list
import pandas as pd
import os
import logging
import requests

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

path = f"{volume_path}/expressions_info.tsv"
_flag = not os.path.isfile(path) or FORCE_DOWNLOAD

if _flag:
  logging.info(f'file {path} does not exist. Downloading expressions_info.tsv')
  download_table(files_endpt,file_fields,path,size=20000,filters=files_filters)
else:
  logging.info(f'file {path} already exists')

files_list_pdf=pd.read_csv(path,sep='\t')
print(f'downloaded {files_list_pdf.shape[0]} records')
files_list_pdf.head()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Download Cases

# COMMAND ----------

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

path = f"{volume_path}/cases.tsv"
download_table(cases_endpt,fields,path,size=100000,filters=cases_filters)

df=spark.read.csv(path,sep='\t',header=True)
print(f"n_records in cases is {df.count()}")
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Download Expressions

# COMMAND ----------

# MAGIC %md
# MAGIC Now we use the list of the files to download expression profiles. Note that we are downloading ~11000 files and this can take some time (even with 32 cores concurrently). 

# COMMAND ----------

EXPRESSION_FILES_PATH=f"{volume_path}/expressions"
dbutils.fs.mkdirs(EXPRESSION_FILES_PATH)
uuids=files_list_pdf.file_id.to_list()
download_expressions(EXPRESSION_FILES_PATH,uuids)


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

path = f"{STAGING_PATH}/cases.tsv"
_flag = not os.path.isfile(path) or FORCE_DOWNLOAD

if _flag:
  logging.info(f'file {path} does not exist. Downloading cases.tsv')
  download_table(cases_endpt,fields,path,size=100000,filters=cases_filters)
else:
  logging.info(f'file {path} already exists')

df=spark.read.csv(path,sep='\t',header=True)
print(f"n_records in cases is {df.count()}")
display(df)
