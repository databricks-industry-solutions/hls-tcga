# Databricks notebook source
dbutils.widgets.text('catalog name','omics_demo')
dbutils.widgets.text('schema name','tcga')

# COMMAND ----------

# DBTITLE 1,Setup paths
user=sql('select current_user() as user').collect()[0].user

staging_path=f"/home/{user}/genomics/data/tcga1/staging"
expression_files_path = f"{staging_path}/expressions"

catalog_name = dbutils.widgets.get('catalog name')
schema_name = dbutils.widgets.get('schema name')

# COMMAND ----------

# DBTITLE 1,create paths if don't exist
dbutils.fs.mkdirs(staging_path)
dbutils.fs.mkdirs(expression_files_path)

# COMMAND ----------

# DBTITLE 1,create configs
configs = {'paths':{'staging_path':staging_path,'expression_files_path':expression_files_path},'catalog':{'ctalog_name':catalog_name,'schema_name':schema_name}}  

# COMMAND ----------

import json
with open('configs.json', 'w') as f:
    json.dump(configs, f)

# COMMAND ----------

displayHTML(f"Configurations:<br>Staging Path: {staging_path}<br>Expression Profiles: {expression_files_path}<br>Catalog Name: {catalog_name}<br>Schema Name: {schema_name}")

# COMMAND ----------


