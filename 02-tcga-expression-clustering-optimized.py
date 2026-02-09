# Databricks notebook source
# MAGIC %md
# MAGIC # Gene Expression Dimensionality Reduction and Clustering (Optimized)
# MAGIC
# MAGIC **Optimized for large-scale data processing with Databricks best practices**
# MAGIC
# MAGIC This notebook demonstrates a workflow for analyzing gene expression profiles using Spark and Python. The main steps include:
# MAGIC * Loading configuration with validation
# MAGIC * Selecting the most variable genes using Spark
# MAGIC * Dimensionality reduction (PCA, t-SNE) with memory optimization
# MAGIC * MLflow experiment tracking
# MAGIC * Visualization and clustering
# MAGIC * Biological interpretation using tissue/organ metadata
# MAGIC
# MAGIC **Key Optimizations:**
# MAGIC - Keeps data in Spark as long as possible
# MAGIC - Widget-based parameterization
# MAGIC - MLflow experiment tracking
# MAGIC - Memory-efficient processing
# MAGIC - Checkpointing for long operations
# MAGIC - Sample size validation before pandas conversion
# MAGIC
# MAGIC ---

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Configuration and Environment Setup

# COMMAND ----------

# Import required libraries
import os
import sys
import json
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from datetime import datetime

# PySpark imports
from pyspark.sql.functions import (
    col, variance, mean, expr, count,
    collect_list, first, current_timestamp, lit
)
from pyspark.sql import Window

# Import configuration utilities
from utils.config_loader import ConfigLoader

# Import MLflow for experiment tracking
import mlflow
import mlflow.sklearn
from mlflow.tracking import MlflowClient

print("✓ All libraries imported successfully")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Load Configuration from Delta Table

# COMMAND ----------

# Create widgets for catalog and schema (passed from job)
dbutils.widgets.text("catalog", "kermany", "Catalog")
dbutils.widgets.text("schema", "tcga", "Schema")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")

if not catalog or not schema:
    raise ValueError("catalog and schema parameters must be provided")

print(f"Loading configuration from {catalog}.{schema}.pipeline_config...")

# Load configuration from Delta table
config_loader = ConfigLoader(spark, catalog, schema)
config = config_loader.get_active_config()

print(f"✓ Loaded configuration ID: {config['config_id']}")
print(f"✓ Timestamp: {config['config_timestamp']}")

# Extract configuration values
config_id = config['config_id']
volume_path = config['volume_path']
database_name = config['database_name']

print(f"Volume path: {volume_path}")
print(f"Database: {database_name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Setup Analysis Parameters

# COMMAND ----------

# Create widgets for notebook parameters
dbutils.widgets.text("n_top_genes", "1000", "Number of Top Variable Genes")
dbutils.widgets.text("n_pca_components", "50", "Number of PCA Components")
dbutils.widgets.text("n_tsne_components", "2", "Number of t-SNE Components")
dbutils.widgets.text("tsne_perplexity", "30", "t-SNE Perplexity")
dbutils.widgets.text("tsne_n_iter", "1000", "t-SNE Iterations")
dbutils.widgets.text("n_clusters", "20", "Number of Clusters")
dbutils.widgets.dropdown("use_sample", "false", ["true", "false"], "Use Sample Data for Testing")
dbutils.widgets.text("sample_size", "1000", "Sample Size (if use_sample=true)")

# Get widget values
N_TOP_GENES = int(dbutils.widgets.get("n_top_genes"))
N_PCA_COMPONENTS = int(dbutils.widgets.get("n_pca_components"))
N_TSNE_COMPONENTS = int(dbutils.widgets.get("n_tsne_components"))
TSNE_PERPLEXITY = int(dbutils.widgets.get("tsne_perplexity"))
TSNE_N_ITER = int(dbutils.widgets.get("tsne_n_iter"))
N_CLUSTERS = int(dbutils.widgets.get("n_clusters"))
USE_SAMPLE = dbutils.widgets.get("use_sample").lower() == "true"
SAMPLE_SIZE = int(dbutils.widgets.get("sample_size"))

print(f"Analysis Parameters:")
print(f"  Top Genes: {N_TOP_GENES}")
print(f"  PCA Components: {N_PCA_COMPONENTS}")
print(f"  t-SNE Components: {N_TSNE_COMPONENTS}")
print(f"  Clusters: {N_CLUSTERS}")
print(f"  Use Sample: {USE_SAMPLE}")

# Get user ID for table naming
user_id = spark.sql('SELECT session_user() AS user').select(
    expr("regexp_replace(split(user, '@')[0], '\\\.', '_') AS user_id")
).collect()[0]['user_id']

# Display configuration
print("="*70)
print("TCGA Expression Clustering Configuration")
print("="*70)
print(f"Database: {database_name}")
print(f"User ID: {user_id}")
print(f"Top Variable Genes: {N_TOP_GENES}")
print(f"PCA Components: {N_PCA_COMPONENTS}")
print(f"t-SNE Components: {N_TSNE_COMPONENTS}")
print(f"t-SNE Perplexity: {TSNE_PERPLEXITY}")
print(f"Number of Clusters: {N_CLUSTERS}")
print(f"Use Sample: {USE_SAMPLE}")
if USE_SAMPLE:
    print(f"Sample Size: {SAMPLE_SIZE}")
print("="*70)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Initialize MLflow Experiment

# COMMAND ----------

# Set up MLflow experiment
experiment_name = f"/Users/{user_id}/tcga-expression-clustering"
mlflow.set_experiment(experiment_name)

# Start MLflow run
mlflow.start_run(run_name=f"clustering_{datetime.now().strftime('%Y%m%d_%H%M%S')}")

# Log parameters
mlflow.log_param("n_top_genes", N_TOP_GENES)
mlflow.log_param("n_pca_components", N_PCA_COMPONENTS)
mlflow.log_param("n_tsne_components", N_TSNE_COMPONENTS)
mlflow.log_param("tsne_perplexity", TSNE_PERPLEXITY)
mlflow.log_param("tsne_n_iter", TSNE_N_ITER)
mlflow.log_param("n_clusters", N_CLUSTERS)
mlflow.log_param("use_sample", USE_SAMPLE)
mlflow.log_param("database", database_name)

print(f"✓ MLflow experiment initialized: {experiment_name}")
print(f"✓ Run ID: {mlflow.active_run().info.run_id}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Load and Prepare Expression Data (Spark-based)

# COMMAND ----------

# Load expression profiles from table
expression_profiles_df = spark.table(f'{database_name}.expression_profiles')

# Log dataset metrics
n_total_samples = expression_profiles_df.select('file_id').distinct().count()
n_total_genes = expression_profiles_df.select('gene_id').distinct().count()

mlflow.log_metric("total_samples", n_total_samples)
mlflow.log_metric("total_genes", n_total_genes)

print(f"Loaded expression profiles:")
print(f"  Total samples: {n_total_samples:,}")
print(f"  Total genes: {n_total_genes:,}")

# Optional: Sample data for faster testing
if USE_SAMPLE:
    # Get random sample of file IDs
    sample_files = (
        expression_profiles_df
        .select('file_id')
        .distinct()
        .sample(fraction=min(1.0, SAMPLE_SIZE / n_total_samples), seed=42)
        .limit(SAMPLE_SIZE)
    )
    expression_profiles_df = expression_profiles_df.join(sample_files, on='file_id', how='inner')
    n_samples = expression_profiles_df.select('file_id').distinct().count()
    print(f"✓ Using sample of {n_samples} samples")
    mlflow.log_metric("sampled_samples", n_samples)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Select Most Variable Genes (Distributed Computation)

# COMMAND ----------

print("Computing gene variance using Spark...")

# Compute variance for each gene based on fpkm_unstranded
gene_var_df = (
    expression_profiles_df
    .groupBy('gene_id')
    .agg(
        variance(col('fpkm_unstranded')).alias('v_fpkm'),
        mean(col('fpkm_unstranded')).alias('mean_fpkm'),
        count('*').alias('n_samples')
    )
    .filter(col('v_fpkm').isNotNull())
)

# Select top N most variable genes
top_genes_df = (
    gene_var_df
    .orderBy(col('v_fpkm').desc())
    .limit(N_TOP_GENES)
)

# Cache for reuse
top_genes_df.cache()
top_genes_list = [row['gene_id'] for row in top_genes_df.select('gene_id').collect()]

# Log gene variance statistics
variance_stats = gene_var_df.select(
    mean('v_fpkm').alias('mean_variance'),
    expr('percentile(v_fpkm, 0.5)').alias('median_variance')
).collect()[0]

mlflow.log_metric("mean_gene_variance", variance_stats['mean_variance'])
mlflow.log_metric("median_gene_variance", variance_stats['median_variance'])
mlflow.log_metric("selected_genes", len(top_genes_list))

print(f"✓ Selected top {len(top_genes_list)} variable genes")
print(f"  Mean variance: {variance_stats['mean_variance']:.4f}")
print(f"  Median variance: {variance_stats['median_variance']:.4f}")

# Display top genes
display(top_genes_df.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Pivot Data to Wide Format (Still in Spark)

# COMMAND ----------

print("Pivoting data to wide format...")

# Filter to top variable genes
filtered_expr_df = expression_profiles_df.filter(col('gene_id').isin(top_genes_list))

# Pivot to wide format: rows = file_id, columns = gene_id, values = fpkm_unstranded
pivot_df = (
    filtered_expr_df
    .groupBy('file_id')
    .pivot('gene_id', top_genes_list)
    .agg(first('fpkm_unstranded'))
)

# Cache the pivoted dataframe
pivot_df.cache()

# Save pivoted data as checkpoint
checkpoint_table = f"{database_name}.{user_id}_expression_profiles_pivoted"
pivot_df.write.mode('overwrite').saveAsTable(checkpoint_table)

print(f"✓ Pivoted data saved to: {checkpoint_table}")
mlflow.log_param("checkpoint_table", checkpoint_table)

# Verify data size before converting to pandas
n_rows = pivot_df.count()
n_cols = len(pivot_df.columns)
data_size_mb = (n_rows * n_cols * 8) / (1024 * 1024)  # Rough estimate in MB

print(f"  Rows: {n_rows:,}")
print(f"  Columns: {n_cols:,}")
print(f"  Estimated size: {data_size_mb:.2f} MB")

# Safety check before pandas conversion
MAX_SIZE_MB = 2000  # 2GB limit
if data_size_mb > MAX_SIZE_MB:
    raise ValueError(
        f"Data size ({data_size_mb:.2f} MB) exceeds maximum ({MAX_SIZE_MB} MB). "
        f"Consider using USE_SAMPLE=true or reducing N_TOP_GENES."
    )

mlflow.log_metric("pivot_rows", n_rows)
mlflow.log_metric("pivot_cols", n_cols)
mlflow.log_metric("data_size_mb", data_size_mb)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Convert to Pandas for ML Operations
# MAGIC
# MAGIC **Note**: Only converting to pandas after size validation and checkpointing.
# MAGIC For very large datasets, consider using Spark MLlib instead.

# COMMAND ----------

print("Converting to pandas (validated size)...")

# Convert to pandas
pivot_pdf = pivot_df.toPandas()

# Extract feature matrix
X = pivot_pdf.select_dtypes(include=[np.number]).values
file_ids = pivot_pdf['file_id'].values

print(f"✓ Converted to pandas")
print(f"  Shape: {X.shape}")
print(f"  Memory usage: {X.nbytes / (1024**2):.2f} MB")

# Check for missing values
n_missing = np.isnan(X).sum()
if n_missing > 0:
    print(f"  Warning: {n_missing} missing values found, filling with 0")
    X = np.nan_to_num(X, nan=0.0)
    mlflow.log_metric("missing_values_filled", n_missing)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Standardize Expression Data

# COMMAND ----------

from sklearn.preprocessing import StandardScaler

print("Standardizing expression data...")

# Standardize features (genes) to have mean=0 and std=1
scaler = StandardScaler()
X_scaled = scaler.fit_transform(X)

print(f"✓ Standardized data")
print(f"  Shape: {X_scaled.shape}")
print(f"  Mean: {np.mean(X_scaled):.6f}")
print(f"  Std: {np.std(X_scaled):.6f}")
print(f"  NaN values: {np.isnan(X_scaled).sum()}")
print(f"  Infinite values: {np.isinf(X_scaled).sum()}")

# Log scaler for reproducibility
mlflow.sklearn.log_model(scaler, "scaler")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Principal Component Analysis (PCA)

# COMMAND ----------

from sklearn.decomposition import PCA

print(f"Applying PCA with {N_PCA_COMPONENTS} components...")

pca = PCA(n_components=N_PCA_COMPONENTS, random_state=42)
X_pca = pca.fit_transform(X_scaled)

explained_variance_ratio = pca.explained_variance_ratio_
cumulative_variance = np.cumsum(explained_variance_ratio)

# Log PCA metrics
mlflow.log_metric("pca_variance_pc1", explained_variance_ratio[0])
mlflow.log_metric("pca_variance_pc2", explained_variance_ratio[1])
mlflow.log_metric("pca_variance_10_components", cumulative_variance[9])
mlflow.log_metric("pca_variance_all_components", cumulative_variance[-1])

# Save PCA model
mlflow.sklearn.log_model(pca, "pca_model")

print(f"✓ PCA complete")
print(f"  PC1 variance: {explained_variance_ratio[0]:.4f}")
print(f"  PC2 variance: {explained_variance_ratio[1]:.4f}")
print(f"  First 10 PCs: {cumulative_variance[9]:.4f}")
print(f"  All {N_PCA_COMPONENTS} PCs: {cumulative_variance[-1]:.4f}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 10. PCA Visualization

# COMMAND ----------

# Create comprehensive PCA visualizations
fig, axes = plt.subplots(2, 2, figsize=(15, 12))

# 1. Scree plot
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
axes[1, 0].scatter(X_pca[:, 0], X_pca[:, 1], alpha=0.6, s=20)
axes[1, 0].set_xlabel(f'PC1 ({explained_variance_ratio[0]*100:.2f}% variance)')
axes[1, 0].set_ylabel(f'PC2 ({explained_variance_ratio[1]*100:.2f}% variance)')
axes[1, 0].set_title('PCA: PC1 vs PC2')
axes[1, 0].grid(True, alpha=0.3)

# 4. PC2 vs PC3 scatter plot
axes[1, 1].scatter(X_pca[:, 1], X_pca[:, 2], alpha=0.6, s=20, color='orange')
axes[1, 1].set_xlabel(f'PC2 ({explained_variance_ratio[1]*100:.2f}% variance)')
axes[1, 1].set_ylabel(f'PC3 ({explained_variance_ratio[2]*100:.2f}% variance)')
axes[1, 1].set_title('PCA: PC2 vs PC3')
axes[1, 1].grid(True, alpha=0.3)

plt.tight_layout()

# Save figure to MLflow
mlflow.log_figure(fig, "pca_analysis.png")
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 11. t-SNE Dimensionality Reduction

# COMMAND ----------

from sklearn.manifold import TSNE
import time

print(f"Applying t-SNE (perplexity={TSNE_PERPLEXITY}, n_iter={TSNE_N_ITER})...")

# Use first 20 PCA components as input to t-SNE for computational efficiency
n_pca_for_tsne = min(20, N_PCA_COMPONENTS)
X_pca_reduced = X_pca[:, :n_pca_for_tsne]

print(f"  Using first {n_pca_for_tsne} PCA components as input")
print(f"  Input shape: {X_pca_reduced.shape}")

# Apply t-SNE with optimized parameters
start_time = time.time()
tsne = TSNE(
    n_components=N_TSNE_COMPONENTS,
    random_state=42,
    perplexity=TSNE_PERPLEXITY,
    n_iter=TSNE_N_ITER,
    learning_rate=200,
    verbose=1
)
X_tsne = tsne.fit_transform(X_pca_reduced)
end_time = time.time()

computation_time = end_time - start_time
mlflow.log_metric("tsne_computation_time_seconds", computation_time)

print(f"✓ t-SNE complete")
print(f"  Result shape: {X_tsne.shape}")
print(f"  Computation time: {computation_time:.2f} seconds")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 12. Save PCA and t-SNE Results

# COMMAND ----------

# Create dataframe with results
results_pdf = pd.DataFrame({
    'file_id': file_ids,
    'pc1': X_pca[:, 0],
    'pc2': X_pca[:, 1],
    'pc3': X_pca[:, 2],
    'tsne_1': X_tsne[:, 0],
    'tsne_2': X_tsne[:, 1] if N_TSNE_COMPONENTS > 1 else 0
})

# Convert to Spark DataFrame and add config_id for traceability
results_df = spark.createDataFrame(results_pdf)
results_df = results_df.withColumn("config_id", lit(config_id))
results_df = results_df.withColumn("analysis_timestamp", current_timestamp())

results_table = f"{database_name}.{user_id}_expression_dimensionality_results"
results_df.write.mode('overwrite').saveAsTable(results_table)

print(f"✓ Results saved to: {results_table}")
print(f"✓ Results linked to configuration ID: {config_id}")
mlflow.log_param("results_table", results_table)
mlflow.log_param("config_id", config_id)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 13. Join with Clinical Metadata

# COMMAND ----------

# Load clinical metadata
cases_diagnoses_df = spark.read.table(f"{database_name}.cases_diagnoses").select(
    "file_id",
    "tissue_or_organ_of_origin",
    "primary_diagnosis",
    "tumor_grade"
)

# Join with dimensionality reduction results
results_with_metadata = results_df.join(cases_diagnoses_df, on="file_id", how="left")

# Convert to pandas for clustering and visualization
results_with_metadata_pdf = results_with_metadata.toPandas()

print(f"✓ Joined with clinical metadata")
print(f"  Total records: {len(results_with_metadata_pdf):,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 14. K-means Clustering

# COMMAND ----------

from sklearn.cluster import KMeans

# Find top N most common tissue types for focused analysis
top_tissues = (
    results_with_metadata_pdf['tissue_or_organ_of_origin']
    .value_counts()
    .nlargest(N_CLUSTERS)
    .index
    .tolist()
)

# Filter to top tissues
results_top_tissues = results_with_metadata_pdf[
    results_with_metadata_pdf['tissue_or_organ_of_origin'].isin(top_tissues)
].copy()

# K-means clustering on t-SNE components
print(f"Performing K-means clustering with {N_CLUSTERS} clusters...")

kmeans = KMeans(n_clusters=N_CLUSTERS, random_state=42, n_init=10)
results_top_tissues['kmeans_cluster'] = kmeans.fit_predict(
    results_top_tissues[['tsne_1', 'tsne_2']].values
)

# Log clustering metrics
mlflow.log_metric("kmeans_inertia", kmeans.inertia_)
mlflow.log_metric("n_clusters", N_CLUSTERS)
mlflow.sklearn.log_model(kmeans, "kmeans_model")

print(f"✓ K-means clustering complete")
print(f"  Inertia: {kmeans.inertia_:.2f}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 15. Save Final Results

# COMMAND ----------

# Convert back to Spark, add config_id, and save
final_results_df = spark.createDataFrame(results_top_tissues)
final_results_df = final_results_df.withColumn("config_id", lit(config_id))
final_results_df = final_results_df.withColumn("analysis_timestamp", current_timestamp())

final_table = f"{database_name}.{user_id}_expression_clustering_final"
final_results_df.write.mode('overwrite').saveAsTable(final_table)

print(f"✓ Final results saved to: {final_table}")
print(f"✓ Results linked to configuration ID: {config_id}")
mlflow.log_param("final_results_table", final_table)
mlflow.log_param("config_id", config_id)

# Display sample
display(final_results_df.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 16. Visualization: t-SNE colored by Tissue Type

# COMMAND ----------

import plotly.express as px

# Create interactive plot
fig = px.scatter(
    results_top_tissues,
    x='tsne_1',
    y='tsne_2',
    color='tissue_or_organ_of_origin',
    hover_data=['file_id', 'kmeans_cluster', 'primary_diagnosis'],
    title=f't-SNE: Colored by Tissue Type (Top {N_CLUSTERS})',
    labels={'tsne_1': 't-SNE 1', 'tsne_2': 't-SNE 2'},
    width=1000,
    height=700
)
fig.update_traces(marker=dict(size=4, opacity=0.7))
fig.update_layout(legend=dict(itemsizing='constant', font=dict(size=10)))

# Save to MLflow
fig.write_html("/tmp/tsne_by_tissue.html")
mlflow.log_artifact("/tmp/tsne_by_tissue.html", "visualizations")

fig.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 17. Visualization: t-SNE colored by Cluster

# COMMAND ----------

# Create interactive plot colored by cluster
fig2 = px.scatter(
    results_top_tissues,
    x='tsne_1',
    y='tsne_2',
    color='kmeans_cluster',
    hover_data=['file_id', 'tissue_or_organ_of_origin', 'primary_diagnosis'],
    title=f't-SNE: Colored by K-means Cluster (k={N_CLUSTERS})',
    labels={'tsne_1': 't-SNE 1', 'tsne_2': 't-SNE 2'},
    width=1000,
    height=700,
    color_continuous_scale='viridis'
)
fig2.update_traces(marker=dict(size=4, opacity=0.7))

# Save to MLflow
fig2.write_html("/tmp/tsne_by_cluster.html")
mlflow.log_artifact("/tmp/tsne_by_cluster.html", "visualizations")

fig2.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 18. Summary Statistics

# COMMAND ----------

# Compute cluster statistics
cluster_stats = (
    results_top_tissues
    .groupby('kmeans_cluster')['tissue_or_organ_of_origin']
    .value_counts()
    .unstack(fill_value=0)
)

print("Cluster composition by tissue type:")
print(cluster_stats)

# Log summary to MLflow
mlflow.log_text(cluster_stats.to_string(), "cluster_composition.txt")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 19. End MLflow Run

# COMMAND ----------

# Log final summary
mlflow.log_metric("final_records_analyzed", len(results_top_tissues))

# End the MLflow run
mlflow.end_run()

print("="*70)
print("Analysis Complete!")
print("="*70)
print(f"Final Results Table: {final_table}")
print(f"MLflow Experiment: {experiment_name}")
print(f"Run ID: {mlflow.active_run().info.run_id if mlflow.active_run() else 'Run ended'}")
print("="*70)

# COMMAND ----------
