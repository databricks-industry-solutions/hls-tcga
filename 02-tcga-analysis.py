# Databricks notebook source
# MAGIC %md
# MAGIC # Interactive Data Analysis
# MAGIC In this notebook, we show a simple analysis of the data by looking at the average daily cigarettes smoked within each primary diagnosis group. We also demonstrate how you can use the `pyspark_ai` API to interact with your data in natural language (assuming access to an OpenAI API key).
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## 0. Configuration

# COMMAND ----------

# DBTITLE 1,install spark ai 
# MAGIC %pip install pyspark_ai

# COMMAND ----------

# MAGIC %run ./util/configurations

# COMMAND ----------

# DBTITLE 1,look at available tables
sql(f'show tables in {CATALOG_NAME}.{SCHEMA_NAME}').display()

# COMMAND ----------

sql(f'describe {CATALOG_NAME}.{SCHEMA_NAME}.diagnoses').display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Analysis 

# COMMAND ----------

# DBTITLE 1,top 20 diagnosis
sql(f"""
    select primary_diagnosis, count(*) as cnt
    from {CATALOG_NAME}.{SCHEMA_NAME}.diagnoses
    group by primary_diagnosis
    order by 2 desc
    limit 20
""").display()

# COMMAND ----------

sql(f'describe {CATALOG_NAME}.{SCHEMA_NAME}.exposures').display()

# COMMAND ----------

# DBTITLE 1,create an exposure history view
sql(f"""
    CREATE OR REPLACE TEMPORARY VIEW EXPOSURE_HISTORY
    AS
    select e.case_id, int(e.cigarettes_per_day), e.alcohol_intensity, int(e.years_smoked), d.primary_diagnosis
    from {CATALOG_NAME}.{SCHEMA_NAME}.exposures e
    join {CATALOG_NAME}.{SCHEMA_NAME}.diagnoses d
    on e.case_id = d.case_id
""")

# COMMAND ----------

# DBTITLE 1,average cigarettes smoked by primary diagnois 
# MAGIC %sql
# MAGIC select primary_diagnosis, avg(int(cigarettes_per_day)) as avg_pack_day_smoked
# MAGIC from EXPOSURE_HISTORY
# MAGIC where cigarettes_per_day is not null
# MAGIC group by primary_diagnosis
# MAGIC order by 2 desc
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Query Using Natural Langauge 
# MAGIC If you have an OpenAI API key, you can use [pyspark_ai](https://github.com/databrickslabs/pyspark-ai) API to interact with any given table using natural language. To specify your API key, you can either enter the key directly in the notebook using the following command, or store your key using [`dbutils.secrets`.](https://docs.databricks.com/en/security/secrets/secrets.html)
# MAGIC To try this feature, remove `%md` in the following cells and run the cell. 

# COMMAND ----------

# DBTITLE 1,enter openAI api key interactively 
# MAGIC %md
# MAGIC ```
# MAGIC import getpass
# MAGIC os.environ["OPENAI_API_KEY"] = getpass.getpass()
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC Alternatively you can securely store your API key using a [secret scope](https://docs.databricks.com/en/security/secrets/secrets.html) and use the key. After setting up your key, replace the `scope` and `key` in the cell bellow and run the notebook. 

# COMMAND ----------

# MAGIC %md 
# MAGIC ```os.environ["OPENAI_API_KEY"] = dbutils.secrets.get(scope="<your scope>", key="<your key>")```

# COMMAND ----------

# MAGIC %md
# MAGIC ```
# MAGIC from pyspark_ai import SparkAI
# MAGIC spark_ai = SparkAI(verbose=True)
# MAGIC spark_ai.activate()  #
# MAGIC df = sql('select * from EXPOSURE_HISTORY')
# MAGIC ```

# COMMAND ----------

# DBTITLE 1,repeat the analysis using natural language
# MAGIC %md
# MAGIC ```
# MAGIC df.ai.plot('plot a bar chart of averegae cigarretes smoked vs primary diagnosis for the top 20 primary diagnosis')
# MAGIC ```
