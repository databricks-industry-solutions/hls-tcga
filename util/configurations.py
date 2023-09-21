# Databricks notebook source
# MAGIC %md
# MAGIC ## Solution Accelerator Configurations

# COMMAND ----------

# DBTITLE 1,notebook params
dbutils.widgets.text('catalog name','db_omics_solacc')
dbutils.widgets.text('schema name','tcga')
dbutils.widgets.text('volume name','tcga_files')

# COMMAND ----------

import logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# COMMAND ----------

# DBTITLE 1,catalog name
# USER=sql('select current_user() as user').collect()[0].user

CATALOG_NAME = dbutils.widgets.get('catalog name')
SCHEMA_NAME = dbutils.widgets.get('schema name')
VOLUME_NAME = dbutils.widgets.get('volume name')

print(f'catalog name:{CATALOG_NAME}\nschema name:{SCHEMA_NAME}\nvolume name:{VOLUME_NAME}')

# COMMAND ----------

# DBTITLE 1,create the catalog
spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG_NAME}")

# COMMAND ----------

# DBTITLE 1,grant access to account users
# Grant create and use catalog permissions for the catalog to all users on the account.
# This also works for other account-level groups and individual users.
spark.sql(f"""
  GRANT CREATE, USE CATALOG
  ON CATALOG {CATALOG_NAME}
  TO `account users`""")

# COMMAND ----------

# DBTITLE 1,create the schema 
# Create a schema in the catalog that was set earlier.
spark.sql(f"""
  CREATE SCHEMA IF NOT EXISTS {CATALOG_NAME}.{SCHEMA_NAME}
  COMMENT 'schmea for TCGA expression profiles and metadata'""")

# COMMAND ----------

# DBTITLE 1,grant access to the schema
#grant access to schema
sql(f"GRANT USE SCHEMA, CREATE TABLE ON SCHEMA {CATALOG_NAME}.{SCHEMA_NAME} TO `account users`")

# COMMAND ----------

sql(f"""
    CREATE VOLUME IF NOT EXISTS {CATALOG_NAME}.{SCHEMA_NAME}.{VOLUME_NAME}
    COMMENT 'managed volum for tcga files'
    """
    )

# COMMAND ----------

# DBTITLE 1,create staging path 
import os

STAGING_PATH = f"/Volumes/{CATALOG_NAME}/{SCHEMA_NAME}/{VOLUME_NAME}"
EXPRESSION_FILES_PATH = f"{STAGING_PATH}/expressions"

if not os.path.exists(EXPRESSION_FILES_PATH):
    os.mkdir(EXPRESSION_FILES_PATH)

# COMMAND ----------

# DBTITLE 1,function to download a given table
import requests
import json
import pandas as pd

cases_endpt = 'https://api.gdc.cancer.gov/cases'
files_endpt = 'https://api.gdc.cancer.gov/files'
data_endpt = 'https://api.gdc.cancer.gov/data/'

def download_table(end_point,fields,output_file,size=100000,filters=None):
# The 'fields' parameter is passed as a comma-separated string of single names.
  resource = end_point.split('/')[-1] #cases vs files
  if resource == 'files':
    if filters:
      _filters = [{'op':f"{m[1]}",'content':{'field':f"{m[0]}",'value':m[2]}} for m in filters] #+[f_program,f_access]
      api_filter = {
          "op":"and",
          "content": _filters
      }
    else:
      api_filter=None

  else:
    
    if filters:
      _filters = [{'op':f"{m[1]}",'content':{'field':f"{m[0]}",'value':m[2]}} for m in filters]
      api_filter = {
          "op":"and",
          "content": _filters
      }
     
    else:
      api_filter=None

  fields = ','.join(fields)
  params = {
      "filters":api_filter,
      "fields": fields,
      "format": "TSV",
      "size": size
  }
  response = requests.post(end_point, json = params)
  with open(output_file,'w') as f:
    f.write(response.content.decode("utf-8"))

# COMMAND ----------

# DBTITLE 1,function to download expression profiles concurrently from GDC
from concurrent.futures import ThreadPoolExecutor
import requests

def download_single_expression(uuid, target_directory_path):
  local_filename = f"{target_directory_path}/{uuid}"
  url = f'{data_endpt}{uuid}'
  with open(local_filename,'w') as f:
    f.write(requests.get(url).content.decode("utf-8"))
  
def download_expressions(target_directory_path,uuids,n_workers=32):
  params = [(uid,target_directory_path) for uid in uuids]
  # Create a ThreadPoolExecutor. The 'with' keyword ensures that the executor is cleaned up when it's done
  with ThreadPoolExecutor(max_workers=n_workers) as executor:
    # Use the executor to map the function to the arguments
    executor.map(lambda pair: download_single_expression(*pair), params)
    # executor.map(download_file, params)
