# Databricks notebook source
# MAGIC %md
# MAGIC **NOTE**: This notebook is created in collaboration with [Databricks Data Science Agent](https://www.databricks.com/blog/introducing-databricks-assistant-data-science-agent)
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ⚠️ **IMPORTANT**: For production workloads and large datasets, please use the optimized version:
# MAGIC **`02-tcga-expression-clustering-optimized.py`**
# MAGIC
# MAGIC The optimized version includes:
# MAGIC - Memory-efficient processing (keeps data in Spark longer)
# MAGIC - MLflow experiment tracking
# MAGIC - Widget-based parameterization
# MAGIC - Data size validation before pandas conversion
# MAGIC - Checkpointing for reproducibility
# MAGIC - Better error handling
# MAGIC
# MAGIC ---

# COMMAND ----------

# MAGIC %md
# MAGIC # Gene Expression Dimensionality Reduction and Clustering
# MAGIC
# MAGIC This notebook demonstrates a workflow for analyzing gene expression profiles using Spark and Python. The main steps include:
# MAGIC * Loading configuration and data
# MAGIC * Selecting the most variable genes
# MAGIC * Dimensionality reduction (PCA, t-SNE)
# MAGIC * Visualization and clustering
# MAGIC * Biological interpretation using tissue/organ metadata
# MAGIC
# MAGIC ---

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Configuration and Environment Setup
# MAGIC
# MAGIC Load workspace, catalog, schema, and user-specific settings from a JSON configuration file. This ensures all downstream operations use the correct data sources and output locations.

# COMMAND ----------

import os
import json
from pyspark.sql.functions import expr

# Determine the absolute path to the JSON file
config_path = os.path.abspath('./config.json')

# Load configurations from the JSON file
with open(config_path, 'r') as file:
    config = json.load(file)

# Access the configurations
catalog = config['lakehouse']['catalog']
schema = config['lakehouse']['schema']
volume = config['lakehouse']['volume']
volume_path = f'/Volumes/{catalog}/{schema}/{volume}'
database_name = f'{catalog}.{schema}'
user_id = spark.sql('SELECT session_user() AS user').select(expr("regexp_replace(split(user, '@')[0], '\\\.', '_') AS user_id")).collect()[0]['user_id']
user_id
# Print or use the configurations as needed
print(catalog, schema, volume, database_name)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Select Most Variable Genes and Pivot Data
# MAGIC
# MAGIC * Compute variance of gene expression (FPKM) across all samples.
# MAGIC * Select the top N most variable genes (default: 1000).
# MAGIC * Pivot the data to a wide format: rows = samples (`file_id`), columns = genes, values = FPKM.
# MAGIC * Prepare the data for downstream dimensionality reduction.

# COMMAND ----------

from pyspark.sql.functions import col, variance, mean, collect_list, array_sort, array_position, expr
expression_profiles_df = spark.table(f'{database_name}.expression_profiles')
# 1. Compute variance for each gene based on fpkm_unstranded
gene_var_df = (
    expression_profiles_df
    .groupBy('gene_id')
    .agg(variance(col('fpkm_unstranded')).alias('v_fpkm'))
)

# 2. Select top N most variable genes
N = 1000  # adjust as needed
top_genes_df = gene_var_df.orderBy(col('v_fpkm').desc()).limit(N).select('gene_id')
top_genes_list = [row['gene_id'] for row in top_genes_df.collect()]

# 3. Filter expression_profiles_df to only top variable genes
filtered_expr_df = expression_profiles_df.filter(col('gene_id').isin(top_genes_list))

# 4. Pivot to wide format: rows = file_id, columns = gene_id, values = fpkm_unstranded
pivot_df = (
    filtered_expr_df
    .groupBy('file_id')
    .pivot('gene_id', top_genes_list)
    .agg(expr('first(fpkm_unstranded)'))
)
pivot_pdf = pivot_df.toPandas()
pivot_pdf.shape
# 5. Save as a new table
# pivot_df.write.mode('overwrite').saveAsTable(f'{database_name}.{user_id}_expression_profiles_top_variable_genes')

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Standardize Expression Data
# MAGIC
# MAGIC Standardize the gene expression matrix so each gene (feature) has mean 0 and standard deviation 1. This is essential for unbiased dimensionality reduction (PCA, t-SNE).

# COMMAND ----------

from sklearn.preprocessing import StandardScaler# Standardize the expression data
import numpy as np
print("Standardizing expression data...")

# Convert to numpy array for processing
X = pivot_pdf.select_dtypes(include=[np.number]).values
print(f"Original data shape: {X.shape}")

# Standardize features (genes) to have mean=0 and std=1
scaler = StandardScaler()
X_scaled = scaler.fit_transform(X)

print(f"Scaled data shape: {X_scaled.shape}")
print(f"Mean of scaled data: {np.mean(X_scaled):.6f}")
print(f"Std of scaled data: {np.std(X_scaled):.6f}")

# Check for any remaining NaN or infinite values
print(f"\nNaN values: {np.isnan(X_scaled).sum()}")
print(f"Infinite values: {np.isinf(X_scaled).sum()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Principal Component Analysis (PCA)
# MAGIC
# MAGIC Apply PCA to the standardized data to reduce dimensionality and capture the main axes of variation. Evaluate the explained variance and visualize the results.

# COMMAND ----------

from sklearn.decomposition import PCA
import numpy as np

print("Applying PCA to standardized expression data...")

n_components = 50
pca = PCA(n_components=n_components, random_state=42)
X_pca = pca.fit_transform(X_scaled)

print(f"PCA transformed data shape: {X_pca.shape}")
print(f"Number of components: {pca.n_components_}")

explained_variance_ratio = pca.explained_variance_ratio_
cumulative_variance = np.cumsum(explained_variance_ratio)

print(f"\nExplained variance by first 10 components:")
for i in range(10):
    print(f"PC{i+1}: {explained_variance_ratio[i]:.4f} ({explained_variance_ratio[i]*100:.2f}%)")

print(f"\nCumulative explained variance:")
print(f"First 10 components: {cumulative_variance[9]:.4f} ({cumulative_variance[9]*100:.2f}%)")
print(f"First 20 components: {cumulative_variance[19]:.4f} ({cumulative_variance[19]*100:.2f}%)")
print(f"All {n_components} components: {cumulative_variance[-1]:.4f} ({cumulative_variance[-1]*100:.2f}%)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. PCA Visualization and Summary
# MAGIC
# MAGIC Visualize PCA results:
# MAGIC * Scree plot (explained variance per component)
# MAGIC * Cumulative explained variance
# MAGIC * Scatter plots of principal components
# MAGIC * Summary of variance explained by top PCs

# COMMAND ----------

import matplotlib.pyplot as plt

# Create comprehensive PCA visualizations
fig, axes = plt.subplots(2, 2, figsize=(15, 12))

# 1. Scree plot - Explained variance ratio
axes[0, 0].plot(range(1, len(explained_variance_ratio) + 1), explained_variance_ratio, 'bo-', markersize=4)
axes[0, 0].set_xlabel('Principal Component')
axes[0, 0].set_ylabel('Explained Variance Ratio')
axes[0, 0].set_title('Scree Plot - Explained Variance by Component')
axes[0, 0].grid(True, alpha=0.3)

# 2. Cumulative explained variance
axes[0, 1].plot(range(1, len(cumulative_variance) + 1), cumulative_variance, 'ro-', markersize=4)
axes[0, 1].axhline(y=0.8, color='k', linestyle='--', alpha=0.7, label='80% variance')
axes[0, 1].set_xlabel('Number of Components')
axes[0, 1].set_ylabel('Cumulative Explained Variance')
axes[0, 1].set_title('Cumulative Explained Variance')
axes[0, 1].legend()
axes[0, 1].grid(True, alpha=0.3)

# 3. PC1 vs PC2 scatter plot
scatter = axes[1, 0].scatter(X_pca[:, 0], X_pca[:, 1], alpha=0.6, s=20)
axes[1, 0].set_xlabel(f'PC1 ({explained_variance_ratio[0]*100:.2f}% variance)')
axes[1, 0].set_ylabel(f'PC2 ({explained_variance_ratio[1]*100:.2f}% variance)')
axes[1, 0].set_title('PCA: PC1 vs PC2')
axes[1, 0].grid(True, alpha=0.3)

# 4. PC2 vs PC3 scatter plot
scatter2 = axes[1, 1].scatter(X_pca[:, 1], X_pca[:, 2], alpha=0.6, s=20, color='orange')
axes[1, 1].set_xlabel(f'PC2 ({explained_variance_ratio[1]*100:.2f}% variance)')
axes[1, 1].set_ylabel(f'PC3 ({explained_variance_ratio[2]*100:.2f}% variance)')
axes[1, 1].set_title('PCA: PC2 vs PC3')
axes[1, 1].grid(True, alpha=0.3)

plt.tight_layout()
plt.show()

print(f"\nPCA Analysis Summary:")
print(f"- Dataset: {X_scaled.shape[0]} samples, {X_scaled.shape[1]} genes")
print(f"- First 3 PCs explain {cumulative_variance[2]*100:.2f}% of variance")
print(f"- First 10 PCs explain {cumulative_variance[9]*100:.2f}% of variance")
print(f"- {n_components} PCs explain {cumulative_variance[-1]*100:.2f}% of variance")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. t-SNE Dimensionality Reduction
# MAGIC
# MAGIC Apply t-SNE to the top PCA components for non-linear dimensionality reduction. This helps reveal clusters and local structure in the data that PCA may miss.

# COMMAND ----------

# Use t-SNE as an alternative to UMAP for non-linear dimensionality reduction
from sklearn.manifold import TSNE
import time

print("Applying t-SNE for non-linear dimensionality reduction...")

# Use first 20 PCA components as input to t-SNE for computational efficiency
n_pca_components = 20
X_pca_reduced = X_pca[:, :n_pca_components]

print(f"Using first {n_pca_components} PCA components as input to t-SNE")
print(f"Input shape for t-SNE: {X_pca_reduced.shape}")

# Apply t-SNE with optimized parameters
start_time = time.time()
tsne_2d = TSNE(n_components=2, random_state=42, perplexity=30, n_iter=1000, learning_rate=200)
X_tsne_2d = tsne_2d.fit_transform(X_pca_reduced)
end_time = time.time()

print(f"t-SNE 2D result shape: {X_tsne_2d.shape}")
print(f"t-SNE computation time: {end_time - start_time:.2f} seconds")
print("t-SNE transformation completed!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Compare PCA and t-SNE Visualizations
# MAGIC
# MAGIC Create side-by-side visualizations of PCA and t-SNE results to compare linear and non-linear dimensionality reduction. Color by principal components to interpret structure.

# COMMAND ----------

# Create comprehensive visualization comparing PCA and t-SNE
fig, axes = plt.subplots(2, 3, figsize=(18, 12))

# Row 1: PCA visualizations
# PC1 vs PC2
axes[0, 0].scatter(X_pca[:, 0], X_pca[:, 1], alpha=0.6, s=15, c='blue')
axes[0, 0].set_xlabel(f'PC1 ({pca.explained_variance_ratio_[0]:.3f} variance)')
axes[0, 0].set_ylabel(f'PC2 ({pca.explained_variance_ratio_[1]:.3f} variance)')
axes[0, 0].set_title('PCA: PC1 vs PC2')
axes[0, 0].grid(True, alpha=0.3)

# PC2 vs PC3
axes[0, 1].scatter(X_pca[:, 1], X_pca[:, 2], alpha=0.6, s=15, c='blue')
axes[0, 1].set_xlabel(f'PC2 ({pca.explained_variance_ratio_[1]:.3f} variance)')
axes[0, 1].set_ylabel(f'PC3 ({pca.explained_variance_ratio_[2]:.3f} variance)')
axes[0, 1].set_title('PCA: PC2 vs PC3')
axes[0, 1].grid(True, alpha=0.3)

# PC1 vs PC3
axes[0, 2].scatter(X_pca[:, 0], X_pca[:, 2], alpha=0.6, s=15, c='blue')
axes[0, 2].set_xlabel(f'PC1 ({pca.explained_variance_ratio_[0]:.3f} variance)')
axes[0, 2].set_ylabel(f'PC3 ({pca.explained_variance_ratio_[2]:.3f} variance)')
axes[0, 2].set_title('PCA: PC1 vs PC3')
axes[0, 2].grid(True, alpha=0.3)

# Row 2: t-SNE visualizations with different color schemes
# t-SNE main plot
axes[1, 0].scatter(X_tsne_2d[:, 0], X_tsne_2d[:, 1], alpha=0.6, s=15, c='red')
axes[1, 0].set_xlabel('t-SNE Component 1')
axes[1, 0].set_ylabel('t-SNE Component 2')
axes[1, 0].set_title('t-SNE: 2D Embedding')
axes[1, 0].grid(True, alpha=0.3)

# t-SNE colored by PC1 values
scatter1 = axes[1, 1].scatter(X_tsne_2d[:, 0], X_tsne_2d[:, 1], 
                             alpha=0.6, s=15, c=X_pca[:, 0], cmap='viridis')
axes[1, 1].set_xlabel('t-SNE Component 1')
axes[1, 1].set_ylabel('t-SNE Component 2')
axes[1, 1].set_title('t-SNE colored by PC1 values')
axes[1, 1].grid(True, alpha=0.3)
plt.colorbar(scatter1, ax=axes[1, 1])

# t-SNE colored by PC2 values
scatter2 = axes[1, 2].scatter(X_tsne_2d[:, 0], X_tsne_2d[:, 1], 
                             alpha=0.6, s=15, c=X_pca[:, 1], cmap='plasma')
axes[1, 2].set_xlabel('t-SNE Component 1')
axes[1, 2].set_ylabel('t-SNE Component 2')
axes[1, 2].set_title('t-SNE colored by PC2 values')
axes[1, 2].grid(True, alpha=0.3)
plt.colorbar(scatter2, ax=axes[1, 2])

plt.tight_layout()
plt.show()

print("\nDimensionality Reduction Summary:")
print(f"- PCA: Linear reduction, first 3 components explain {cumulative_variance[2]:.1%} of variance")
print(f"- t-SNE: Non-linear reduction, better preserves local structure and reveals clusters")
print(f"- Dataset: {X.shape[0]} samples, {X.shape[1]} genes")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Prepare t-SNE Results for Clustering
# MAGIC
# MAGIC Convert t-SNE results to a DataFrame and associate each sample with its `file_id` for downstream merging and annotation.

# COMMAND ----------

import pandas as pd
tsne_df = pd.DataFrame(X_tsne_2d, columns=['tsne_1', 'tsne_2'])
tsne_df['file_id'] = pivot_pdf['file_id'].values

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Cluster and Visualize by Tissue Type
# MAGIC
# MAGIC * Merge t-SNE results with tissue/organ metadata.
# MAGIC * Identify the top 20 most common tissue types.
# MAGIC * Apply K-means clustering to t-SNE coordinates.
# MAGIC * Visualize clusters and tissue types interactively using Plotly.

# COMMAND ----------

from sklearn.cluster import KMeans
import plotly.express as px

cases_diagnoses_df = spark.read.table(f"{database_name}.cases_diagnoses").select("file_id", "tissue_or_organ_of_origin")
cases_diagnoses_pd = cases_diagnoses_df.toPandas()

# Merge t-SNE results with tissue/organ of origin info
tsne_df_merged = tsne_df.merge(cases_diagnoses_pd, on="file_id", how="left")

# Find top 20 most common tissue types
top_tissues = tsne_df_merged['tissue_or_organ_of_origin'].value_counts().nlargest(20).index.tolist()
tsne_top = tsne_df_merged[tsne_df_merged['tissue_or_organ_of_origin'].isin(top_tissues)].copy()

# K-means clustering on t-SNE components
kmeans = KMeans(n_clusters=20, random_state=42)
tsne_top['kmeans_cluster'] = kmeans.fit_predict(tsne_top[['tsne_1', 'tsne_2']].values)

display(tsne_top[:10])
tsne_top_spark_df = spark.createDataFrame(tsne_top)
tsne_top_spark_df.write.mode("overwrite").saveAsTable(f"{database_name}.tsne_top")
# Visualize: color by tissue type using Plotly Express
fig = px.scatter(
    tsne_top,
    x='tsne_1',
    y='tsne_2',
    color='tissue_or_organ_of_origin',
    hover_data=['file_id', 'kmeans_cluster'],
    title='t-SNE: Colored by Tissue Type (Top 20)',
    labels={'tsne_1': 't-SNE 1', 'tsne_2': 't-SNE 2'},
    width=900,
    height=700
)
fig.update_traces(marker=dict(size=3, opacity=0.7))
fig.update_layout(legend=dict(itemsizing='constant', font=dict(size=10)))
fig.show()

# COMMAND ----------


