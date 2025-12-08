spark.sql("USE SCHEMA gold")
# ============================================================
# IMDb GOLD LAYER - DELTA LIVE TABLES (STREAMING)
# ============================================================
# Gold Layer: Business-ready dimensional model with streaming
# SCD Type 2 only for DIM_PERSON to track changes
# Region/Language names from CSV reference files
# ============================================================

import dlt
from pyspark.sql.functions import (
    col, lit, when, trim, explode, current_timestamp, 
    coalesce, concat, lower,size,array_join
)
from pyspark.sql.types import StringType

# Volume path for reference CSVs
VOLUME_PATH = "/Volumes/imdb_data/bronze/raw"

# ============================================================
# DIM_PERSON - SCD TYPE 2 (with view + apply_changes)
# ============================================================
@dlt.view(
    name="dim_person_changes",
    comment="Change feed for person dimension SCD Type 2"
)
def dim_person_changes():
    return (
        dlt.read_stream("silver.silver_name_basics")
        .select(
            col("nconst"),
            col("primary_name").alias("primaryName"),
            col("birth_year").alias("birthYear"),
            col("death_year").alias("deathYear"),
            array_join(col("primary_professions"), ",").alias("primaryProfession"),
            size(col("primary_professions")).alias("profession_count"),
            col("known_for_titles").alias("knownForTitles"),
            current_timestamp().alias("load_dt"),
            lit("silver_name_basics").alias("source_file")
        )
    )

dlt.create_streaming_table(
    name="gold_dim_person",
    comment="Person dimension with SCD Type 2",
    table_properties={"quality": "gold"}
)

dlt.apply_changes(
    target="gold_dim_person",
    source="dim_person_changes",
    keys=["nconst"],
    sequence_by="load_dt",
    stored_as_scd_type=2,
    track_history_column_list=["primaryName", "deathYear", "primaryProfession"]
)

# ============================================================
# DIM_TITLE - Streaming
# ============================================================

@dlt.table(
    name="gold_dim_title",
    comment="Title dimension",
    table_properties={"quality": "gold"}
)
def gold_dim_title():
    return (
        dlt.read_stream("silver.silver_title_basics")
        .select(
            col("tconst"),
            col("title_type").alias("titleType"),
            col("primary_title").alias("primaryTitle"),
            col("original_title").alias("originalTitle"),
            col("is_adult").alias("isAdult"),
            col("start_year").alias("startYear"),
            col("end_year").alias("endYear"),
            col("runtime_minutes").alias("runtimeMinutes"),
            current_timestamp().alias("load_dt"),
            lit("silver_title_basics").alias("source_file")
        )
    )

# ============================================================
# DIMENSION TABLES - STREAMING
# ============================================================

@dlt.table(
    name="gold_dim_genre",
    comment="Genre dimension",
    table_properties={"quality": "gold"}
)
def gold_dim_genre():
    return (
        dlt.read_stream("silver.silver_title_basics")
        .select(explode(col("genres")).alias("genre_name"))
        .filter(col("genre_name").isNotNull())
        .select(
            trim(col("genre_name")).alias("genre_key"),
            trim(col("genre_name")).alias("genre_name"),
            current_timestamp().alias("load_dt")
        )
        .dropDuplicates(["genre_key"])
    )

@dlt.table(
    name="gold_dim_category",
    comment="Category dimension",
    table_properties={"quality": "gold"}
)
def gold_dim_category():
    return (
        dlt.read_stream("silver.silver_title_principals")
        .select("category")
        .filter(col("category").isNotNull())
        .select(
            trim(col("category")).alias("category_key"),
            col("category").alias("category_name"),
            when(col("category").isin("actor", "actress", "self"), "cast")
                .when(col("category").isin("director", "cinematographer"), "directing")
                .when(col("category").isin("producer", "production_designer"), "production")
                .when(col("category").isin("writer", "editor"), "writing")
                .otherwise("other").alias("department"),
            current_timestamp().alias("load_dt")
        )
        .dropDuplicates(["category_key"])
    )

@dlt.table(
    name="gold_dim_job",
    comment="Job dimension",
    table_properties={"quality": "gold"}
)
def gold_dim_job():
    return (
        dlt.read_stream("silver.silver_title_principals")
        .select("category", "job")
        .filter(col("job").isNotNull())
        .select(
            concat(col("category"), lit("_"), col("job")).alias("job_key"),
            trim(col("category")).alias("category_key"),
            col("job").alias("job_name"),
            col("job").alias("job_description"),
            current_timestamp().alias("load_dt")
        )
        .dropDuplicates(["job_key"])
    )
# ============================================================
# DIM_REGION - Join with CSV reference file
# ============================================================

@dlt.table(
    name="gold_dim_region",
    comment="Region dimension with ISO country names from CSV",
    table_properties={"quality": "gold"}
)
def gold_dim_region():
    ref_countries = (
        spark.read
        .option("header", "true")
        .csv(f"{VOLUME_PATH}/iso_country_codes.csv")
        .select(
            lower(col("region_code")).alias("ref_code"),
            col("region_name").alias("ref_name")
        )
        .dropDuplicates(["ref_code"])  # dedupe CSV
    )
    
    regions = (
        dlt.read("silver.silver_title_akas")
        .select("region")
        .filter(col("region").isNotNull())
        .select(
            (trim(col("region"))).alias("region_key"),
            lower(trim(col("region"))).alias("region_code_lower")
        )
        .dropDuplicates(["region_key"])
    )
    
    return (
        regions
        .join(ref_countries, regions.region_code_lower == ref_countries.ref_code, "left")
        .select(
            col("region_key"),
            col("region_key").alias("region_code"),
            coalesce(col("ref_name"), col("region_key")).alias("region_name"),
            current_timestamp().alias("load_dt")
        )
        .dropDuplicates(["region_key"])  # final dedupe
    )

# ============================================================
# DIM_LANGUAGE - Join with CSV reference file
# ============================================================

@dlt.table(
    name="gold_dim_language",
    comment="Language dimension with ISO language names from CSV",
    table_properties={"quality": "gold"}
)
def gold_dim_language():
    ref_languages = (
        spark.read
        .option("header", "true")
        .csv(f"{VOLUME_PATH}/iso_language_codes.csv")
        .select(
            lower(col("language_code")).alias("ref_code"),
            col("language_name").alias("ref_name")
        )
        .dropDuplicates(["ref_code"])  # dedupe CSV
    )
    
    languages = (
        dlt.read("silver.silver_title_akas")
        .select("language")
        .filter(col("language").isNotNull())
        .select(
            (trim(col("language"))).alias("language_key"),
            lower(trim(col("language"))).alias("language_code_lower")
        )
        .dropDuplicates(["language_key"])
    )
    
    return (
        languages
        .join(ref_languages, languages.language_code_lower == ref_languages.ref_code, "left")
        .select(
            col("language_key"),
            col("language_key").alias("language_code"),
            coalesce(col("ref_name"), col("language_key")).alias("language_name"),
            current_timestamp().alias("load_dt")
        )
        .dropDuplicates(["language_key"])  # final dedupe
    )

# ============================================================
# DIM_TITLE_ALTERNATES
# ============================================================
@dlt.table(
    name="gold_dim_title_alternates",
    comment="Alternative titles by region and language",
    table_properties={"quality": "gold"}
)
def gold_dim_title_alternates():
    return (
        dlt.read_stream("silver.silver_title_akas")
        .select(
            concat(col("title_id"), lit("_"), col("ordering")).alias("alternate_key"),  # PK
            col("title_id").alias("tconst"),
            trim(col("region")).alias("region_key"),
            trim(col("language")).alias("language_key"),
            col("title").alias("localized_title"),
            col("types").alias("release_type"),
            col("attributes"),
            col("is_original_title"),
            col("ordering"),
            current_timestamp().alias("load_dt"),
            lit("silver_title_akas").alias("source_file")
        )
    )

# ============================================================
# BRIDGE TABLE - STREAMING
# ============================================================

@dlt.table(
    name="gold_bridge_title_genre",
    comment="Bridge table for many-to-many title-genre relationship",
    table_properties={"quality": "gold"}
)
def gold_bridge_title_genre():
    return (
        dlt.read_stream("silver.silver_title_basics")
        .select(
            col("tconst"),
            explode(col("genres")).alias("genre_name")
        )
        .filter(col("genre_name").isNotNull())
        .select(
            col("tconst"),
            trim(col("genre_name")).alias("genre_key"),
            lit(False).alias("is_primary_genre"),
            current_timestamp().alias("load_dt")
        )
    )

# ============================================================
# FACT TABLES - STREAMING
# ============================================================

@dlt.table(
    name="gold_fact_ratings",
    comment="Ratings fact table",
    table_properties={"quality": "gold", "pipelines.autoOptimize.managed": "true"}
)
def gold_fact_ratings():
    return (
        dlt.read_stream("silver.silver_title_ratings")
        .select(
            col("tconst"),
            col("average_rating"),
            col("num_votes"),
            current_timestamp().alias("load_dt"),
            lit("silver_title_ratings").alias("source_file"),
            lit("dlt_pipeline").alias("loaded_by")
        )
    )

@dlt.table(
    name="gold_fact_episodes",
    comment="Episodes fact table",
    table_properties={"quality": "gold", "pipelines.autoOptimize.managed": "true"}
)
def gold_fact_episodes():
    return (
        dlt.read_stream("silver.silver_title_episode")
        .select(
            col("tconst").alias("episode_tconst"),
            col("parent_tconst").alias("series_tconst"),
            col("season_number"),
            col("episode_number"),
            current_timestamp().alias("load_dt"),
            lit("silver_title_episode").alias("source_file"),
            lit("dlt_pipeline").alias("loaded_by")
        )
    )
@dlt.table(
    name="gold_fact_title_credits",
    comment="Title credits fact table",
    table_properties={"quality": "gold", "pipelines.autoOptimize.managed": "true"}
)
def gold_fact_title_credits():
    return (
        dlt.read_stream("silver.silver_title_principals")
        .select(
            col("tconst"),
            col("nconst"),
            trim(col("category")).alias("category_key"),
            concat(col("category"), lit("_"), coalesce(col("job"), lit(""))).alias("job_key"),
            col("ordering"),
            col("characters"),
            current_timestamp().alias("load_dt"),
            lit("silver_title_principals").alias("source_file"),
            lit("dlt_pipeline").alias("loaded_by")
        )
    )