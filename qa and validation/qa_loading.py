# Databricks notebook source
# ============================================================
# IMDb QA LAYER - DELTA LIVE TABLES
# Pipeline Target Schema: qa
# Reads from bronze schema tables
# ============================================================
spark.sql("USE SCHEMA qa")

import dlt
from pyspark.sql.functions import (
    col, lit, count, current_timestamp, array, max as spark_max,
    sum as spark_sum, when, concat_ws, collect_list, struct
)

# ============================================================
# CONFIGURATION
# ============================================================

CATALOG = "imdb_data"
BRONZE_SCHEMA = "bronze"

TABLES_CONFIG = [
    ("bronze_name_basics", "nconst", ["nconst", "primaryName", "birthYear", "deathYear", "primaryProfession", "knownForTitles"]),
    ("bronze_title_basics", "tconst", ["tconst", "titleType", "primaryTitle", "originalTitle", "isAdult", "startYear", "endYear", "runtimeMinutes", "genres"]),
    ("bronze_title_ratings", "tconst", ["tconst", "averageRating", "numVotes"]),
    ("bronze_title_episode", "tconst", ["tconst", "parentTconst", "seasonNumber", "episodeNumber"]),
    ("bronze_title_akas", "titleId,ordering", ["titleId", "ordering", "title", "region", "language", "types", "attributes", "isOriginalTitle"]),
    ("bronze_title_principals", "tconst,ordering", ["tconst", "ordering", "nconst", "category", "job", "characters"]),
    ("bronze_title_crew", "tconst", ["tconst", "directors", "writers"])
]


# ============================================================
# QA: LOAD AUDIT TABLE
# ============================================================

@dlt.table(
    name="qa_load_audit",
    comment="Audit log for all Bronze table loads"
)
def qa_load_audit():
    audit_dfs = []
    
    for table_name, pk_cols, expected_cols in TABLES_CONFIG:
        full_table = f"{CATALOG}.{BRONZE_SCHEMA}.{table_name}"
        df = spark.read.table(full_table)
        
        actual_cols = [c for c in df.columns if not c.startswith("_")]
        new_cols = [c for c in actual_cols if c not in expected_cols]
        
        audit_df = df.agg(
            lit(table_name).alias("table_name"),
            lit(f"{table_name.replace('bronze_', '')}.tsv.gz").alias("file_name"),
            lit("SUCCESS").alias("status"),
            lit(None).cast("string").alias("error_message"),
            count("*").alias("rows_loaded"),
            spark_max("_loaded_at").alias("load_timestamp")
        ).withColumn(
            "columns_detected", lit(",".join(actual_cols))
        ).withColumn(
            "new_columns_added", lit(",".join(new_cols) if new_cols else "")
        ).withColumn(
            "schema_drift_detected", lit(len(new_cols) > 0)
        )
        
        audit_dfs.append(audit_df)
    
    result = audit_dfs[0]
    for df in audit_dfs[1:]:
        result = result.union(df)
    
    return result


# ============================================================
# QA: ROW COUNT HISTORY
# ============================================================

@dlt.table(
    name="qa_row_count_history",
    comment="Row count history for Bronze tables"
)
def qa_row_count_history():
    history_dfs = []
    
    for table_name, pk_cols, expected_cols in TABLES_CONFIG:
        full_table = f"{CATALOG}.{BRONZE_SCHEMA}.{table_name}"
        df = spark.read.table(full_table)
        
        history_df = df.agg(
            lit(table_name).alias("table_name"),
            count("*").alias("row_count"),
            current_timestamp().alias("recorded_at")
        ).withColumn(
            "delta_from_previous", lit(0).cast("bigint")
        )
        
        history_dfs.append(history_df)
    
    result = history_dfs[0]
    for df in history_dfs[1:]:
        result = result.union(df)
    
    return result


# ============================================================
# QA: DATA QUALITY TABLE (FULLY DYNAMIC)
# ============================================================

@dlt.table(
    name="qa_data_quality",
    comment="Data quality metrics for Bronze tables"
)
def qa_data_quality():
    quality_dfs = []
    
    for table_name, pk_cols_str, expected_cols in TABLES_CONFIG:
        full_table = f"{CATALOG}.{BRONZE_SCHEMA}.{table_name}"
        df = spark.read.table(full_table)
        pk_cols = pk_cols_str.split(",")
        data_cols = [c for c in df.columns if not c.startswith("_")]
        
        # Build null count expressions dynamically
        null_exprs = [
            spark_sum(when(col(c).isNull(), 1).otherwise(0)).alias(f"null_{c}")
            for c in data_cols
        ]
        
        # Get total count and null counts in one pass
        stats_df = df.agg(
            count("*").alias("total_rows"),
            *null_exprs
        )
        
        # Calculate duplicate count
        if len(pk_cols) == 1:
            dup_df = df.groupBy(pk_cols[0]).count().filter("count > 1").agg(
                count("*").alias("duplicate_count")
            )
        else:
            dup_df = df.groupBy(*pk_cols).count().filter("count > 1").agg(
                count("*").alias("duplicate_count")
            )
        
        # Build the quality DataFrame using SQL functions
        quality_df = stats_df.crossJoin(dup_df).select(
            lit(table_name).alias("table_name"),
            col("total_rows"),
            col("duplicate_count"),
            lit(pk_cols_str).alias("primary_key_columns"),
            current_timestamp().alias("check_timestamp")
        )
        
        # Add null counts as a concatenated string
        null_col_exprs = [
            concat_ws(":", lit(c), col(f"null_{c}").cast("string"))
            for c in data_cols
        ]
        
        quality_df = stats_df.crossJoin(dup_df).select(
            lit(table_name).alias("table_name"),
            col("total_rows"),
            concat_ws(",", *null_col_exprs).alias("null_counts"),
            col("duplicate_count"),
            lit(pk_cols_str).alias("primary_key_columns"),
            current_timestamp().alias("check_timestamp")
        )
        
        # Calculate quality score dynamically
        # Start with 100, subtract 20 if duplicates exist
        quality_df = quality_df.withColumn(
            "quality_score",
            when(col("duplicate_count") > 0, lit(80.0)).otherwise(lit(100.0))
        )
        
        # Add issues found
        quality_df = quality_df.withColumn(
            "issues_found",
            when(col("duplicate_count") > 0, 
                 concat_ws("|", lit("DUPLICATE_PK"), col("duplicate_count").cast("string"))
            ).otherwise(lit(""))
        )
        
        quality_dfs.append(quality_df)
    
    result = quality_dfs[0]
    for df in quality_dfs[1:]:
        result = result.union(df)
    
    return result