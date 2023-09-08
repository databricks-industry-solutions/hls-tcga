# Databricks notebook source
# DBTITLE 1,setup widgets 
dbutils.widgets.text('catalog name','omics_demo')
dbutils.widgets.text('schema name','tcga')

# COMMAND ----------

# DBTITLE 1,setup paths
user=sql('select current_user() as user').collect()[0].user

catalog_name = dbutils.widgets.get('catalog name')
schema_name = dbutils.widgets.get('schema name')

staging_path = f"/home/{user}/{catalog_name}/{schema_name}/staging"
expression_files_path = f"{staging_path}/expressions"



# COMMAND ----------

# DBTITLE 1,create paths
dbutils.fs.mkdirs(staging_path)
dbutils.fs.mkdirs(expression_files_path)

# COMMAND ----------

# DBTITLE 1,create configs
import json

configs = {'paths':{'staging_path':staging_path,'expression_files_path':expression_files_path},'catalog':{'ctalog_name':catalog_name,'schema_name':schema_name}} 

with open('configs.json', 'w') as f:
    json.dump(configs, f)

displayHTML(f"Configurations:<br>Staging Path: {staging_path}<br>Expression Profiles: {expression_files_path}<br>Catalog Name: {catalog_name}<br>Schema Name: {schema_name}")
