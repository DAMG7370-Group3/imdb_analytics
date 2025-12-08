# ============================================================
# IMDb QA SCRIPT - SILVER TO GOLD VALIDATION
# ============================================================
# Validates Facts, Dimensions, Bridge tables, and SCD Type 2
# Run this notebook in Databricks after Gold layer pipeline
# ============================================================

from pyspark.sql.functions import *
from pyspark.sql.types import *
from datetime import datetime

# Configuration
SILVER_SCHEMA = "silver"
GOLD_SCHEMA = "gold"

# ============================================================
# HELPER FUNCTIONS
# ============================================================

def print_header(title):
    print("\n" + "="*60)
    print(f" {title}")
    print("="*60)

def print_result(test_name, passed, details=""):
    status = "✅ PASS" if passed else "❌ FAIL"
    print(f"{status} | {test_name}")
    if details:
        print(f"       └── {details}")

def print_warning(test_name, details=""):
    print(f"⚠️ WARN | {test_name}")
    if details:
        print(f"       └── {details}")

# ============================================================
# 1. DIMENSION TABLE VALIDATION
# ============================================================

def validate_dimensions():
    print_header("1. DIMENSION TABLE VALIDATION")
    
    # DIM_PERSON (SCD Type 2)
    df_person = spark.table(f"{GOLD_SCHEMA}.gold_dim_person")
    person_count = df_person.count()
    person_distinct = df_person.select("nconst").distinct().count()
    print_result("gold_dim_person loaded", person_count > 0, f"Total rows: {person_count:,} | Distinct nconst: {person_distinct:,}")
    
    # Check SCD2 columns exist
    scd2_cols = ["__START_AT", "__END_AT"]
    existing_cols = df_person.columns
    has_scd2 = all(c in existing_cols for c in scd2_cols)
    print_result("gold_dim_person has SCD2 columns", has_scd2, f"Columns: {scd2_cols}")
    
    # DIM_TITLE
    df_title = spark.table(f"{GOLD_SCHEMA}.gold_dim_title")
    title_count = df_title.count()
    silver_title_count = spark.table(f"{SILVER_SCHEMA}.silver_title_basics").count()
    match_pct = (title_count / silver_title_count * 100) if silver_title_count > 0 else 0
    print_result("gold_dim_title count", match_pct >= 95, f"Gold: {title_count:,} | Silver: {silver_title_count:,} ({match_pct:.1f}%)")
    
    # DIM_GENRE
    df_genre = spark.table(f"{GOLD_SCHEMA}.gold_dim_genre")
    genre_count = df_genre.count()
    print_result("gold_dim_genre loaded", genre_count > 0, f"Distinct genres: {genre_count:,}")
    genre_nulls = df_genre.filter(col("genre_key").isNull()).count()
    print_result("gold_dim_genre no null keys", genre_nulls == 0, f"Null keys: {genre_nulls:,}")
    
    # DIM_CATEGORY
    df_cat = spark.table(f"{GOLD_SCHEMA}.gold_dim_category")
    cat_count = df_cat.count()
    print_result("gold_dim_category loaded", cat_count > 0, f"Distinct categories: {cat_count:,}")
    
    # Validate department mapping
    valid_depts = ["cast", "directing", "production", "writing", "other"]
    invalid_depts = df_cat.filter(~col("department").isin(valid_depts)).count()
    print_result("gold_dim_category valid departments", invalid_depts == 0, f"Invalid: {invalid_depts:,}")
    
    # DIM_JOB
    df_job = spark.table(f"{GOLD_SCHEMA}.gold_dim_job")
    job_count = df_job.count()
    print_result("gold_dim_job loaded", job_count > 0, f"Distinct jobs: {job_count:,}")
    
    # DIM_REGION
    df_region = spark.table(f"{GOLD_SCHEMA}.gold_dim_region")
    region_count = df_region.count()
    print_result("gold_dim_region loaded", region_count > 0, f"Distinct regions: {region_count:,}")
    
    # Check region name lookup worked
    unnamed_regions = df_region.filter(col("region_name") == col("region_key")).count()
    named_pct = ((region_count - unnamed_regions) / region_count * 100) if region_count > 0 else 0
    print_result("gold_dim_region name lookup", named_pct >= 80, f"Named: {named_pct:.1f}% | Fallback to code: {unnamed_regions:,}")
    
    # DIM_LANGUAGE
    df_lang = spark.table(f"{GOLD_SCHEMA}.gold_dim_language")
    lang_count = df_lang.count()
    print_result("gold_dim_language loaded", lang_count > 0, f"Distinct languages: {lang_count:,}")
    
    # DIM_TITLE_ALTERNATES
    df_alts = spark.table(f"{GOLD_SCHEMA}.gold_dim_title_alternates")
    alt_count = df_alts.count()
    print_result("gold_dim_title_alternates loaded", alt_count > 0, f"Total alternates: {alt_count:,}")

# ============================================================
# 2. FACT TABLE VALIDATION
# ============================================================

def validate_facts():
    print_header("2. FACT TABLE VALIDATION")
    
    # FACT_RATINGS
    print("\n  -- FACT_RATINGS --")
    df_ratings = spark.table(f"{GOLD_SCHEMA}.gold_fact_ratings")
    silver_ratings = spark.table(f"{SILVER_SCHEMA}.silver_title_ratings")
    
    gold_cnt = df_ratings.count()
    silver_cnt = silver_ratings.count()
    match_pct = (gold_cnt / silver_cnt * 100) if silver_cnt > 0 else 0
    print_result("Row count match", match_pct >= 99, f"Gold: {gold_cnt:,} | Silver: {silver_cnt:,} ({match_pct:.1f}%)")
    
    # Validate rating range
    invalid_ratings = df_ratings.filter(
        (col("average_rating") < 1.0) | (col("average_rating") > 10.0)
    ).count()
    print_result("average_rating in [1.0, 10.0]", invalid_ratings == 0, f"Invalid: {invalid_ratings:,}")
    
    # Validate votes
    invalid_votes = df_ratings.filter(col("num_votes") <= 0).count()
    print_result("num_votes > 0", invalid_votes == 0, f"Invalid: {invalid_votes:,}")
    
    # Null FK check
    null_fk = df_ratings.filter(col("tconst").isNull()).count()
    print_result("No null tconst FK", null_fk == 0, f"Null FKs: {null_fk:,}")
    
    # FACT_EPISODES
    print("\n  -- FACT_EPISODES --")
    df_episodes = spark.table(f"{GOLD_SCHEMA}.gold_fact_episodes")
    
    ep_count = df_episodes.count()
    print_result("gold_fact_episodes loaded", ep_count > 0, f"Total episodes: {ep_count:,}")
    
    # Validate season/episode numbers
    invalid_season = df_episodes.filter(
        (col("season_number").isNotNull()) & (col("season_number") < 0)
    ).count()
    invalid_episode = df_episodes.filter(
        (col("episode_number").isNotNull()) & (col("episode_number") < 0)
    ).count()
    print_result("season_number >= 0", invalid_season == 0, f"Invalid: {invalid_season:,}")
    print_result("episode_number >= 0", invalid_episode == 0, f"Invalid: {invalid_episode:,}")
    
    # Check episode != series
    self_ref = df_episodes.filter(col("episode_tconst") == col("series_tconst")).count()
    print_result("episode_tconst != series_tconst", self_ref == 0, f"Self-refs: {self_ref:,}")
    
    # FACT_TITLE_CREDITS
    print("\n  -- FACT_TITLE_CREDITS --")
    df_credits = spark.table(f"{GOLD_SCHEMA}.gold_fact_title_credits")
    
    credit_count = df_credits.count()
    print_result("gold_fact_title_credits loaded", credit_count > 0, f"Total credits: {credit_count:,}")
    
    # Null FK checks
    null_tconst = df_credits.filter(col("tconst").isNull()).count()
    null_nconst = df_credits.filter(col("nconst").isNull()).count()
    print_result("No null tconst FK", null_tconst == 0, f"Null: {null_tconst:,}")
    print_result("No null nconst FK", null_nconst == 0, f"Null: {null_nconst:,}")
    
    # Ordering validation
    invalid_order = df_credits.filter(col("ordering") <= 0).count()
    print_result("ordering > 0", invalid_order == 0, f"Invalid: {invalid_order:,}")

# ============================================================
# 3. BRIDGE TABLE VALIDATION
# ============================================================

def validate_bridge():
    print_header("3. BRIDGE TABLE VALIDATION")
    
    df_bridge = spark.table(f"{GOLD_SCHEMA}.gold_bridge_title_genre")
    
    bridge_count = df_bridge.count()
    print_result("gold_bridge_title_genre loaded", bridge_count > 0, f"Total rows: {bridge_count:,}")
    
    # Check for null FKs
    null_tconst = df_bridge.filter(col("tconst").isNull()).count()
    null_genre = df_bridge.filter(col("genre_key").isNull()).count()
    print_result("No null tconst", null_tconst == 0, f"Null: {null_tconst:,}")
    print_result("No null genre_key", null_genre == 0, f"Null: {null_genre:,}")
    
    # Validate genre keys exist in dim_genre
    df_genre = spark.table(f"{GOLD_SCHEMA}.gold_dim_genre")
    orphan_genres = df_bridge.join(
        df_genre, df_bridge.genre_key == df_genre.genre_key, "left_anti"
    ).count()
    print_result("All genre_keys exist in dim_genre", orphan_genres == 0, f"Orphans: {orphan_genres:,}")
    
    # Check duplicates
    total = df_bridge.count()
    distinct = df_bridge.select("tconst", "genre_key").distinct().count()
    duplicates = total - distinct
    print_result("No duplicate (tconst, genre_key)", duplicates == 0, f"Duplicates: {duplicates:,}")

# ============================================================
# 4. REFERENTIAL INTEGRITY VALIDATION
# ============================================================

def validate_referential_integrity():
    print_header("4. REFERENTIAL INTEGRITY VALIDATION")
    
    df_title = spark.table(f"{GOLD_SCHEMA}.gold_dim_title")
    df_person = spark.table(f"{GOLD_SCHEMA}.gold_dim_person")
    df_category = spark.table(f"{GOLD_SCHEMA}.gold_dim_category")
    df_region = spark.table(f"{GOLD_SCHEMA}.gold_dim_region")
    df_language = spark.table(f"{GOLD_SCHEMA}.gold_dim_language")
    
    # FACT_RATINGS → DIM_TITLE
    df_ratings = spark.table(f"{GOLD_SCHEMA}.gold_fact_ratings")
    orphan_ratings = df_ratings.join(df_title, "tconst", "left_anti").count()
    print_result("fact_ratings.tconst → dim_title", orphan_ratings == 0, f"Orphans: {orphan_ratings:,}")
    
    # FACT_EPISODES → DIM_TITLE (episode)
    df_episodes = spark.table(f"{GOLD_SCHEMA}.gold_fact_episodes")
    orphan_ep = df_episodes.join(
        df_title, df_episodes.episode_tconst == df_title.tconst, "left_anti"
    ).count()
    print_result("fact_episodes.episode_tconst → dim_title", orphan_ep == 0, f"Orphans: {orphan_ep:,}")
    
    # FACT_EPISODES → DIM_TITLE (series)
    orphan_series = df_episodes.join(
        df_title, df_episodes.series_tconst == df_title.tconst, "left_anti"
    ).count()
    print_result("fact_episodes.series_tconst → dim_title", orphan_series == 0, f"Orphans: {orphan_series:,}")
    
    # FACT_CREDITS → DIM_TITLE
    df_credits = spark.table(f"{GOLD_SCHEMA}.gold_fact_title_credits")
    orphan_credit_title = df_credits.join(df_title, "tconst", "left_anti").count()
    print_result("fact_credits.tconst → dim_title", orphan_credit_title == 0, f"Orphans: {orphan_credit_title:,}")
    
    # FACT_CREDITS → DIM_PERSON
    orphan_credit_person = df_credits.join(df_person, "nconst", "left_anti").count()
    pct_orphan = (orphan_credit_person / df_credits.count() * 100) if df_credits.count() > 0 else 0
    # Allow some orphans due to SCD2 timing
    print_result("fact_credits.nconst → dim_person", pct_orphan < 1, f"Orphans: {orphan_credit_person:,} ({pct_orphan:.2f}%)")
    
    # FACT_CREDITS → DIM_CATEGORY
    orphan_credit_cat = df_credits.join(
        df_category, df_credits.category_key == df_category.category_key, "left_anti"
    ).count()
    print_result("fact_credits.category_key → dim_category", orphan_credit_cat == 0, f"Orphans: {orphan_credit_cat:,}")
    
    # DIM_TITLE_ALTERNATES → DIM_REGION
    df_alts = spark.table(f"{GOLD_SCHEMA}.gold_dim_title_alternates")
    orphan_region = df_alts.filter(col("region_key").isNotNull()).join(
        df_region, df_alts.region_key == df_region.region_key, "left_anti"
    ).count()
    print_result("dim_alternates.region_key → dim_region", orphan_region == 0, f"Orphans: {orphan_region:,}")
    
    # DIM_TITLE_ALTERNATES → DIM_LANGUAGE
    orphan_lang = df_alts.filter(col("language_key").isNotNull()).join(
        df_language, df_alts.language_key == df_language.language_key, "left_anti"
    ).count()
    print_result("dim_alternates.language_key → dim_language", orphan_lang == 0, f"Orphans: {orphan_lang:,}")

# ============================================================
# 5. SCD TYPE 2 VALIDATION (DIM_PERSON)
# ============================================================

def validate_scd2():
    print_header("5. SCD TYPE 2 VALIDATION (DIM_PERSON)")
    
    df = spark.table(f"{GOLD_SCHEMA}.gold_dim_person")
    
    # Check for tracked columns
    tracked_cols = ["primaryName", "deathYear", "primaryProfession"]
    existing = df.columns
    has_tracked = all(c in existing for c in tracked_cols)
    print_result("Tracked columns exist", has_tracked, f"Columns: {tracked_cols}")
    
    # Check SCD2 metadata columns
    if "__START_AT" in df.columns and "__END_AT" in df.columns:
        # Current records (END_AT is null)
        current_records = df.filter(col("__END_AT").isNull()).count()
        historical_records = df.filter(col("__END_AT").isNotNull()).count()
        total = df.count()
        
        print_result("Has current records", current_records > 0, f"Current: {current_records:,}")
        print(f"       └── Historical records: {historical_records:,}")
        print(f"       └── Total records: {total:,}")
        
        # Check no overlapping periods for same nconst
        overlap_check = df.alias("a").join(
            df.alias("b"),
            (col("a.nconst") == col("b.nconst")) &
            (col("a.__START_AT") < col("b.__END_AT")) &
            (col("a.__END_AT") > col("b.__START_AT")) &
            (col("a.__START_AT") != col("b.__START_AT"))
        ).count()
        print_result("No overlapping SCD2 periods", overlap_check == 0, f"Overlaps: {overlap_check:,}")
    else:
        print_warning("SCD2 columns not found", "Expected __START_AT and __END_AT")

# ============================================================
# 6. DATA QUALITY METRICS
# ============================================================

def validate_data_quality():
    print_header("6. DATA QUALITY METRICS")
    
    # DIM_TITLE completeness
    df_title = spark.table(f"{GOLD_SCHEMA}.gold_dim_title")
    total = df_title.count()
    
    null_checks = {
        "primaryTitle": df_title.filter(col("primaryTitle").isNull()).count(),
        "titleType": df_title.filter(col("titleType").isNull()).count(),
        "startYear": df_title.filter(col("startYear").isNull()).count(),
    }
    
    print("\n  DIM_TITLE Completeness:")
    for col_name, null_cnt in null_checks.items():
        complete_pct = ((total - null_cnt) / total * 100) if total > 0 else 0
        print(f"    {col_name}: {complete_pct:.1f}% complete ({null_cnt:,} nulls)")
    
    # DIM_PERSON completeness
    df_person = spark.table(f"{GOLD_SCHEMA}.gold_dim_person")
    total_person = df_person.count()
    
    person_nulls = {
        "primaryName": df_person.filter(col("primaryName").isNull()).count(),
        "birthYear": df_person.filter(col("birthYear").isNull()).count(),
        "primaryProfession": df_person.filter(col("primaryProfession").isNull()).count(),
    }
    
    print("\n  DIM_PERSON Completeness:")
    for col_name, null_cnt in person_nulls.items():
        complete_pct = ((total_person - null_cnt) / total_person * 100) if total_person > 0 else 0
        print(f"    {col_name}: {complete_pct:.1f}% complete ({null_cnt:,} nulls)")
    
    # FACT_RATINGS stats
    df_ratings = spark.table(f"{GOLD_SCHEMA}.gold_fact_ratings")
    stats = df_ratings.agg(
        avg("average_rating").alias("avg_rating"),
        min("average_rating").alias("min_rating"),
        max("average_rating").alias("max_rating"),
        avg("num_votes").alias("avg_votes"),
        sum("num_votes").alias("total_votes")
    ).collect()[0]
    
    print("\n  FACT_RATINGS Statistics:")
    print(f"    Avg Rating: {stats['avg_rating']:.2f}")
    print(f"    Rating Range: {stats['min_rating']:.1f} - {stats['max_rating']:.1f}")
    print(f"    Avg Votes: {stats['avg_votes']:,.0f}")
    print(f"    Total Votes: {stats['total_votes']:,}")

# ============================================================
# 7. DUPLICATE KEY VALIDATION
# ============================================================

def validate_duplicates():
    print_header("7. DUPLICATE KEY VALIDATION")
    
    checks = [
        ("gold_dim_title", ["tconst"]),
        ("gold_dim_genre", ["genre_key"]),
        ("gold_dim_category", ["category_key"]),
        ("gold_dim_job", ["job_key"]),
        ("gold_dim_region", ["region_key"]),
        ("gold_dim_language", ["language_key"]),
        ("gold_dim_title_alternates", ["alternate_key"]),
        ("gold_fact_ratings", ["tconst"]),
    ]
    
    for table, pk_cols in checks:
        df = spark.table(f"{GOLD_SCHEMA}.{table}")
        total = df.count()
        distinct = df.select(pk_cols).distinct().count()
        duplicates = total - distinct
        passed = duplicates == 0
        print_result(f"{table}", passed, f"Duplicates: {duplicates:,}")

# ============================================================
# 8. FRESHNESS VALIDATION
# ============================================================

def validate_freshness():
    print_header("8. DATA FRESHNESS VALIDATION")
    
    tables = [
        "gold_dim_title", "gold_dim_genre", "gold_dim_category",
        "gold_dim_region", "gold_dim_language", "gold_dim_title_alternates",
        "gold_bridge_title_genre", "gold_fact_ratings", "gold_fact_episodes", "gold_fact_title_credits"
    ]
    
    for table in tables:
        try:
            df = spark.table(f"{GOLD_SCHEMA}.{table}")
            if "load_dt" in df.columns:
                latest = df.agg(max("load_dt")).collect()[0][0]
                if latest:
                    age_hours = (datetime.now() - latest.replace(tzinfo=None)).total_seconds() / 3600
                    passed = age_hours < 24
                    print_result(f"{table}", passed, f"Last load: {latest} ({age_hours:.1f}h ago)")
                else:
                    print_warning(f"{table}", "No load_dt values found")
            else:
                print_warning(f"{table}", "No load_dt column")
        except Exception as e:
            print_result(f"{table}", False, f"Error: {str(e)}")

# ============================================================
# MAIN EXECUTION
# ============================================================

def run_all_qa_checks():
    print("\n" + "="*60)
    print(" IMDb SILVER → GOLD QA VALIDATION REPORT")
    print(f" Run Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("="*60)
    
    validate_dimensions()
    validate_facts()
    validate_bridge()
    validate_referential_integrity()
    validate_scd2()
    validate_data_quality()
    validate_duplicates()
    validate_freshness()
    
    print("\n" + "="*60)
    print(" QA VALIDATION COMPLETE")
    print("="*60)

# Run all QA checks
run_all_qa_checks()