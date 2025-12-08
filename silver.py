# ============================================================
# IMDb SILVER LAYER - DELTA LIVE TABLES (STREAMING)
# ============================================================
spark.sql("USE SCHEMA silver")
# ============================================================

# ============================================================
# IMDb SILVER LAYER - DELTA LIVE TABLES (STREAMING)
# Pipeline Target Schema: silver
# Cleansed, typed, and validated data from Bronze
# ============================================================

import dlt
from pyspark.sql.functions import (
    col, lit, when, trim, upper, lower, regexp_replace, split,
    current_timestamp
)
from pyspark.sql.types import IntegerType, DoubleType



# ============================================================
# VALID ENUMERATIONS (Based on IMDb Documentation)
# ============================================================

VALID_TITLE_TYPES = [
    "movie", "short", "tvSeries", "tvEpisode", "tvMiniSeries", 
    "tvMovie", "tvSpecial", "tvShort", "video", "videoGame", "tvPilot"
]

# ============================================================
# RANGE CONSTANTS
# ============================================================

MIN_TITLE_YEAR = 1850
MAX_TITLE_YEAR = 2035


# ============================================================
# SILVER: NAME BASICS
# - BC years stored as positive integers (427 = 427 BC)
# ============================================================

@dlt.table(
    name="silver_name_basics",
    comment="Cleansed person/talent data",
    table_properties={"quality": "silver",        "delta.enableChangeDataFeed": "true"}
)
@dlt.expect_or_drop("valid_nconst", "nconst IS NOT NULL AND nconst LIKE 'nm%'")
@dlt.expect_or_drop("valid_nconst_format", "LENGTH(nconst) BETWEEN 9 AND 12")
@dlt.expect("valid_birth_year", "birth_year IS NULL OR birth_year > 0")
@dlt.expect("valid_death_year", "death_year IS NULL OR death_year > 0")
@dlt.expect_or_drop("valid_name", "primary_name IS NOT NULL AND LENGTH(primary_name) > 1")
def silver_name_basics():
    return (
        dlt.read_stream("bronze.bronze_name_basics")
        .select(
            trim(col("nconst")).alias("nconst"),
            trim(col("primaryName")).alias("primary_name"),
            when(col("birthYear").rlike("^[0-9]{1,4}$"), col("birthYear").cast(IntegerType())).alias("birth_year"),
            when(col("deathYear").rlike("^[0-9]{1,4}$"), col("deathYear").cast(IntegerType())).alias("death_year"),
            when(col("primaryProfession").isNotNull() & (col("primaryProfession") != ""),
                split(trim(col("primaryProfession")), ",")).alias("primary_professions"),
            when(col("knownForTitles").isNotNull() & (col("knownForTitles") != ""),
                split(trim(col("knownForTitles")), ",")).alias("known_for_titles"),
            col("_loaded_at").alias("source_loaded_at"),
            current_timestamp().alias("silver_processed_at")
        )
    )


# ============================================================
# SILVER: TITLE BASICS
# - Future years allowed (announced films)
# - No upper limit on runtime (e.g., 146,445 min for documentaries)
# ============================================================

@dlt.table(
    name="silver_title_basics",
    comment="Cleansed title data",
    table_properties={"quality": "silver",        "delta.enableChangeDataFeed": "true"}
)
@dlt.expect_or_drop("valid_tconst", "tconst IS NOT NULL AND tconst LIKE 'tt%'")
@dlt.expect_or_drop("valid_tconst_format", "LENGTH(tconst) BETWEEN 9 AND 12")
@dlt.expect("valid_title_type", f"title_type IN ({','.join([repr(t) for t in VALID_TITLE_TYPES])})")
@dlt.expect("valid_start_year", f"start_year IS NULL OR (start_year >= {MIN_TITLE_YEAR} AND start_year <= {MAX_TITLE_YEAR})")
@dlt.expect("valid_end_year", f"end_year IS NULL OR (end_year >= {MIN_TITLE_YEAR} AND end_year <= {MAX_TITLE_YEAR})")
@dlt.expect("valid_runtime", "runtime_minutes IS NULL OR runtime_minutes > 0")
@dlt.expect("start_before_end", "start_year IS NULL OR end_year IS NULL OR start_year <= end_year")
@dlt.expect("valid_is_adult", "is_adult IN (true, false)")
@dlt.expect("has_primary_title", "primary_title IS NOT NULL AND LENGTH(primary_title) >= 1")
def silver_title_basics():
    return (
        dlt.read_stream("bronze.bronze_title_basics")
        .select(
            trim(col("tconst")).alias("tconst"),
            trim(col("titleType")).alias("title_type"),
            trim(col("primaryTitle")).alias("primary_title"),
            trim(col("originalTitle")).alias("original_title"),
            when(col("isAdult") == "1", lit(True)).otherwise(lit(False)).alias("is_adult"),
            when(col("startYear").rlike("^[0-9]{4}$"), col("startYear").cast(IntegerType())).alias("start_year"),
            when(col("endYear").rlike("^[0-9]{4}$"), col("endYear").cast(IntegerType())).alias("end_year"),
            when(col("runtimeMinutes").rlike("^[0-9]+$"), col("runtimeMinutes").cast(IntegerType())).alias("runtime_minutes"),
            when(col("genres").isNotNull() & (col("genres") != ""),
                split(trim(col("genres")), ",")).alias("genres"),
            col("_loaded_at").alias("source_loaded_at"),
            current_timestamp().alias("silver_processed_at")
        )
    )


# ============================================================
# SILVER: TITLE RATINGS
# ============================================================

@dlt.table(
    name="silver_title_ratings",
    comment="Cleansed ratings data",
    table_properties={"quality": "silver",        "delta.enableChangeDataFeed": "true"}
)
@dlt.expect_or_drop("valid_tconst", "tconst IS NOT NULL AND tconst LIKE 'tt%'")
@dlt.expect_or_drop("valid_rating_range", "average_rating >= 1.0 AND average_rating <= 10.0")
@dlt.expect_or_drop("valid_num_votes", "num_votes > 0")
def silver_title_ratings():
    return (
        dlt.read_stream("bronze.bronze_title_ratings")
        .select(
            trim(col("tconst")).alias("tconst"),
            col("averageRating").cast(DoubleType()).alias("average_rating"),
            col("numVotes").cast(IntegerType()).alias("num_votes"),
            col("_loaded_at").alias("source_loaded_at"),
            current_timestamp().alias("silver_processed_at")
        )
    )


# ============================================================
# SILVER: TITLE EPISODE
# - No upper limit on seasons (e.g., "Imphy, capitale de la France" has 500+)
# - No upper limit on episodes
# ============================================================

@dlt.table(
    name="silver_title_episode",
    comment="Cleansed episode data",
    table_properties={"quality": "silver",        "delta.enableChangeDataFeed": "true"}
)
@dlt.expect_or_drop("valid_tconst", "tconst IS NOT NULL AND tconst LIKE 'tt%'")
@dlt.expect_or_drop("valid_parent_tconst", "parent_tconst IS NOT NULL AND parent_tconst LIKE 'tt%'")
@dlt.expect("valid_season_number", "season_number IS NULL OR season_number >= 0")
@dlt.expect("valid_episode_number", "episode_number IS NULL OR episode_number >= 0")
@dlt.expect("episode_not_same_as_parent", "tconst != parent_tconst")
def silver_title_episode():
    return (
        dlt.read_stream("bronze.bronze_title_episode")
        .select(
            trim(col("tconst")).alias("tconst"),
            trim(col("parentTconst")).alias("parent_tconst"),
            when(col("seasonNumber").rlike("^[0-9]+$"), col("seasonNumber").cast(IntegerType())).alias("season_number"),
            when(col("episodeNumber").rlike("^[0-9]+$"), col("episodeNumber").cast(IntegerType())).alias("episode_number"),
            col("_loaded_at").alias("source_loaded_at"),
            current_timestamp().alias("silver_processed_at")
        )
    )


# ============================================================
# SILVER: TITLE AKAS
# ============================================================

@dlt.table(
    name="silver_title_akas",
    comment="Cleansed alternative titles",
    table_properties={"quality": "silver",        "delta.enableChangeDataFeed": "true"}
)
@dlt.expect_or_drop("valid_title_id", "title_id IS NOT NULL AND title_id LIKE 'tt%'")
@dlt.expect_or_drop("valid_ordering", "ordering IS NOT NULL AND ordering > 0")
@dlt.expect("has_title", "title IS NOT NULL AND LENGTH(title) >= 1")
@dlt.expect("valid_is_original", "is_original_title IN (true, false)")
def silver_title_akas():
    return (
        dlt.read_stream("bronze.bronze_title_akas")
        .select(
            trim(col("titleId")).alias("title_id"),
            col("ordering").cast(IntegerType()).alias("ordering"),
            trim(col("title")).alias("title"),
            upper(trim(col("region"))).alias("region"),
            lower(trim(col("language"))).alias("language"),
            when(col("types").isNotNull() & (col("types") != ""),
                split(trim(col("types")), ",")).alias("types"),
            trim(col("attributes")).alias("attributes"),
            when(col("isOriginalTitle") == "1", lit(True)).otherwise(lit(False)).alias("is_original_title"),
            col("_loaded_at").alias("source_loaded_at"),
            current_timestamp().alias("silver_processed_at")
        )
    )


# ============================================================
# SILVER: TITLE PRINCIPALS
# - Category validated as clean string (lowercase letters/underscores only)
# ============================================================

@dlt.table(
    name="silver_title_principals",
    comment="Cleansed cast/crew data",
    table_properties={"quality": "silver",        "delta.enableChangeDataFeed": "true"}
)
@dlt.expect_or_drop("valid_tconst", "tconst IS NOT NULL AND tconst LIKE 'tt%'")
@dlt.expect_or_drop("valid_nconst", "nconst IS NOT NULL AND nconst LIKE 'nm%'")
@dlt.expect_or_drop("valid_ordering", "ordering IS NOT NULL AND ordering > 0")
@dlt.expect("valid_category", "category IS NOT NULL AND category RLIKE '^[a-z_]+$'")
def silver_title_principals():
    return (
        dlt.read_stream("bronze.bronze_title_principals")
        .select(
            trim(col("tconst")).alias("tconst"),
            col("ordering").cast(IntegerType()).alias("ordering"),
            trim(col("nconst")).alias("nconst"),
            lower(trim(col("category"))).alias("category"),
            trim(col("job")).alias("job"),
            regexp_replace(
                regexp_replace(trim(col("characters")), "^\\[\"", ""),
                "\"\\]$", ""
            ).alias("characters"),
            col("_loaded_at").alias("source_loaded_at"),
            current_timestamp().alias("silver_processed_at")
        )
    )


# ============================================================
# SILVER: TITLE CREW
# ============================================================

@dlt.table(
    name="silver_title_crew",
    comment="Cleansed crew data",
    table_properties={"quality": "silver",        "delta.enableChangeDataFeed": "true"}
)
@dlt.expect_or_drop("valid_tconst", "tconst IS NOT NULL AND tconst LIKE 'tt%'")
def silver_title_crew():
    return (
        dlt.read_stream("bronze.bronze_title_crew")
        .select(
            trim(col("tconst")).alias("tconst"),
            when(col("directors").isNotNull() & (col("directors") != ""),
                split(trim(col("directors")), ",")).alias("directors"),
            when(col("writers").isNotNull() & (col("writers") != ""),
                split(trim(col("writers")), ",")).alias("writers"),
            col("_loaded_at").alias("source_loaded_at"),
            current_timestamp().alias("silver_processed_at")
        )
    )