# Databricks notebook source
# MAGIC %md
# MAGIC # Expression Profile Clustering
# MAGIC
# MAGIC In this notebook, we explore expression profiles for each sample and inspect clusters of samples.
# MAGIC To do so, we select a subset of genes with the highest variability across all samples and use [UMAP](https://umap-learn.readthedocs.io/en/latest/) for dimensionality reduction, reducing the number of features to 2 for visual inspection.
# MAGIC We then interactively label each sample on the 2D plot based on different clinical features.
# MAGIC
# MAGIC [![](https://mermaid.ink/img/pako:eNplk11v2jAUhv_KkSeUG1rRrtXWXEyCANvFQNM-ehNXkYlPUkv-iGynowL--04SKFSLosQ-53nfcxLbO1Y6iSxlo9GOWwBlVUyhHwIk8RkNJikkGxEwGV9GH4VXYqMxJG84pRqvjPCvmdPOd7oPd7cPD4vJSXomfuM2nqmqqv5HZs5L9GfoUzah64LTyuI5Pbn7eH8_v0gHLJ2V77r5PM2my-UFE9FH9Q6ZTRc3yywZiEP3osdhNOK29qJ5hu8_uZ3mNVosNL6gLnDbeAxBOVuEKGJ4gqvrqy-wD6ixjJzb6BpYg3EhAs1ejv8NOouwhywvXWtjgMp5Sq9h0KEcgCduZ_lFica7StFfP1VZQRCm0b0Tt9kxGr2wgQwNOa7Au7-hty6dbo0N-3m-2q4pYkT0aksl5oNusfuzmv6g7128uR-10BUtlByXNyCshPK2Mwv7ZY5mg1IqW3etfs2lErV14dxgVCG0WDhPdy1s4SoaqFpZcj0udHEUKfIDbpeD8lveNUNUqdtA69Tx2sWuDBszg94IJWnj9puPs35TcpbSUGIlWh05o4UjtG2kiLiQKjrP0uhbHDPRRvfr1Zan-cDMqQ8vDEsroQNFsdeshgPSn5PDPyQQDsY?type=png)](https://mermaid.live/edit#pako:eNplk11v2jAUhv_KkSeUG1rRrtXWXEyCANvFQNM-ehNXkYlPUkv-iGynowL--04SKFSLosQ-53nfcxLbO1Y6iSxlo9GOWwBlVUyhHwIk8RkNJikkGxEwGV9GH4VXYqMxJG84pRqvjPCvmdPOd7oPd7cPD4vJSXomfuM2nqmqqv5HZs5L9GfoUzah64LTyuI5Pbn7eH8_v0gHLJ2V77r5PM2my-UFE9FH9Q6ZTRc3yywZiEP3osdhNOK29qJ5hu8_uZ3mNVosNL6gLnDbeAxBOVuEKGJ4gqvrqy-wD6ixjJzb6BpYg3EhAs1ejv8NOouwhywvXWtjgMp5Sq9h0KEcgCduZ_lFica7StFfP1VZQRCm0b0Tt9kxGr2wgQwNOa7Au7-hty6dbo0N-3m-2q4pYkT0aksl5oNusfuzmv6g7128uR-10BUtlByXNyCshPK2Mwv7ZY5mg1IqW3etfs2lErV14dxgVCG0WDhPdy1s4SoaqFpZcj0udHEUKfIDbpeD8lveNUNUqdtA69Tx2sWuDBszg94IJWnj9puPs35TcpbSUGIlWh05o4UjtG2kiLiQKjrP0uhbHDPRRvfr1Zan-cDMqQ8vDEsroQNFsdeshgPSn5PDPyQQDsY)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 0. Configurations 

# COMMAND ----------

# DBTITLE 1,install umap
# MAGIC %pip install umap-learn

# COMMAND ----------

# MAGIC %run ./util/configurations

# COMMAND ----------

# DBTITLE 1,create widgets
sample_labels = ['primary_diagnosis','tissue_or_organ_of_origin','tumor_grade']
dbutils.widgets.dropdown('sample_label','tissue_or_organ_of_origin',sample_labels)

# COMMAND ----------

# DBTITLE 1,set up paths
database_name = f'{CATALOG_NAME}.{SCHEMA_NAME}'
print(database_name)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Data Preparation
# MAGIC Now we proceed tp prepare expression profiles data for clustering. Note that the tcga expressions are normalized using three commonly used methods:
# MAGIC
# MAGIC 1. FPKM 
# MAGIC   The fragments per kilobase of transcript per million mapped reads (FPKM) calculation aims to control for transcript length and overall sequencing quantity.
# MAGIC
# MAGIC 2. Upper Quartile FPKM 
# MAGIC   The upper quartile FPKM (FPKM-UQ) is a modified FPKM calculation in which the protein coding gene in the 75th percentile position is substituted for the sequencing quantity. This is thought to provide a more stable value than including the noisier genes at the extremes.
# MAGIC
# MAGIC 3.  TPM 
# MAGIC   The transcripts per million calculation is similar to FPKM, but the difference is that all transcripts are normalized for length first. Then, instead of using the total overall read count as a normalization for size, the sum of the length-normalized transcript values are used as an indicator of size.
# MAGIC
# MAGIC In the following analysis we use FPKM values for exploring clusters. 

# COMMAND ----------

# DBTITLE 1,load summary statistics data 
sample_stats_df= sql(f'select * from {database_name}.sample_level_expression_stats')
gene_stats_df = sql(f'select * from {database_name}.gene_level_expression_stats')

# COMMAND ----------

# DBTITLE 1,look at sample-level distributions
sample_stats_df.display()

# COMMAND ----------

gene_stats_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Select the Most Variable Genes
# MAGIC Since we are primarily interested in clustering samples based on their expression counts, we first select a subset of genes that have high variability among different samples and then apply dimensionality reduction on this subset of features.

# COMMAND ----------

from pyspark.sql.functions import col 

selected_genes = gene_stats_df.orderBy(col('v_fpkm').desc()).limit(20000).select('gene_id')

# COMMAND ----------

# MAGIC %md
# MAGIC ### Convert Expression Profiles
# MAGIC Now, we transform the expression profiles so that each row corresponds to a given sample, and each column corresponds to the selected genes.
# MAGIC

# COMMAND ----------

from pyspark.sql.functions import pandas_udf, collect_list
from pyspark.ml.linalg import Vectors, VectorUDT
from pyspark.sql import DataFrame
from pyspark.sql.functions import pandas_udf, PandasUDFType
import pandas as pd

def get_expression_lists(df: DataFrame) -> DataFrame:

  return(
    df
    .groupBy('file_id')
    .agg(
      collect_list(col('fpkm_unstranded')).alias('fpkm_list'),
      collect_list(col('fpkm_uq_unstranded')).alias('fpkm_uq_list'),
      collect_list(col('tpm_unstranded')).alias('tpm_list'),
    )
  )

# COMMAND ----------

expressions_df=sql(f'select * from {database_name}.expression_profiles')
expressions_vec_df = expressions_df.join(selected_genes,on='gene_id').transform(get_expression_lists)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. UMAP Plots
# MAGIC Now that the data is ready we can proceed to calculate expression embeddings using UMAP and plot the embeddings. 

# COMMAND ----------

# DBTITLE 1,UMAP parameters 
n_neighbors=15
min_dist=0.1
n_components = 2
params ={'n_neighbors':n_neighbors,
               'min_dist':min_dist,
               'n_components':n_components,
          }

# COMMAND ----------

# DBTITLE 1,embedding calculation
def get_embeddings(df,feature_col,params):
  import umap
  _pdf=df.select('file_id',feature_col).toPandas()
  m=_pdf.shape[0]
  n=len(_pdf[feature_col][0])
  features_pdf=pd.DataFrame(np.concatenate(_pdf[feature_col]).reshape(m,n))
  mapper = umap.UMAP(**params).fit(features_pdf)
  out_pdf = pd.DataFrame(mapper.embedding_,columns=['c1','c2'])
  out_pdf['file_id']=_pdf['file_id']
  out_spdf=spark.createDataFrame(out_pdf).pandas_api()
  return(out_spdf)

# COMMAND ----------

# MAGIC %md
# MAGIC Now we create embeddings based on `fpkm` column. We can also repeat this analysis using other columns (`fpkm_uq_list` or `tpm_list`) 

# COMMAND ----------

# DBTITLE 1,create a pandas on spark dataframe of embeddings 
import numpy as np
expression_embedding_spdf = get_embeddings(expressions_vec_df,'fpkm_list',params)

# COMMAND ----------

expression_embedding_spdf.shape

# COMMAND ----------

# DBTITLE 1,embeddings dataset 
expression_embedding_spdf.head()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Combine with Clinical Data
# MAGIC Now we add clinical data to be used for labeling each sample in the UMAP plot. 

# COMMAND ----------

diagnoses_spdf=sql(f'select * from {database_name}.diagnoses').pandas_api()
expression_profiles_diagnoses_spdf = expression_embedding_spdf.merge(diagnoses_spdf, on='file_id')
expression_profiles_diagnoses_spdf.head()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create UMAP Visulization 
# MAGIC Now we are ready for visualizing the clusters. To better explore these clusters and determine which clinical features of the sample the resulting clusters correspond to, you can change choose the target label from the dropdown menu on top of this notebook (`sample_label`)

# COMMAND ----------

expression_profiles_diagnoses_spdf.display()

# COMMAND ----------

df=expression_profiles_diagnoses_spdf.to_pandas()
# Find the top 10 most common diagnoses
top_10_diagnoses = df['tissue_or_organ_of_origin'].value_counts().head(10).index

# Filter the rows corresponding to the top 10 diagnoses
filtered_df = df[df['tissue_or_organ_of_origin'].isin(top_10_diagnoses)]

# COMMAND ----------

import plotly.express as px
label = dbutils.widgets.get('sample_label')
fig = px.scatter(filtered_df,x='c1',y='c2',hover_name='primary_diagnosis',color=label,width=1200,height=1000)
fig.update_traces(marker=dict(size=3))
fig.show()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC As you can see, the identified clusters correspond to `tissue_or_organ_of_origin`. Now, go ahead and change the `sample_label` in the widget on this notebook and select `primary_diagnosis`. You will see that the colors still correspond to the detected clusters, but to a lesser degree.
# MAGIC
