# ============================================================
# IMDb QA SCRIPT - BRONZE TO SILVER VALIDATION
# ============================================================
# Run this notebook in Databricks to validate Bronze → Silver
# data quality, row counts, and transformation accuracy
# ============================================================

from pyspark.sql.functions import *
from pyspark.sql.types import *
from datetime import datetime

# Configuration
BRONZE_SCHEMA = "bronze"
SILVER_SCHEMA = "silver"

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

def get_row_counts(bronze_table, silver_table):
    bronze_count = spark.table(f"{BRONZE_SCHEMA}.{bronze_table}").count()
    silver_count = spark.table(f"{SILVER_SCHEMA}.{silver_table}").count()
    return bronze_count, silver_count

# ============================================================
# 1. ROW COUNT VALIDATION
# ============================================================

def validate_row_counts():
    print_header("1. ROW COUNT VALIDATION (Bronze vs Silver)")
    
    tables = [
        ("bronze_name_basics", "silver_name_basics"),
        ("bronze_title_basics", "silver_title_basics"),
        ("bronze_title_ratings", "silver_title_ratings"),
        ("bronze_title_episode", "silver_title_episode"),
        ("bronze_title_akas", "silver_title_akas"),
        ("bronze_title_principals", "silver_title_principals"),
        ("bronze_title_crew", "silver_title_crew"),
    ]
    
    results = []
    for bronze_tbl, silver_tbl in tables:
        bronze_cnt, silver_cnt = get_row_counts(bronze_tbl, silver_tbl)
        dropped = bronze_cnt - silver_cnt
        drop_pct = (dropped / bronze_cnt * 100) if bronze_cnt > 0 else 0
        
        # Allow up to 5% drop due to data quality expectations
        passed = drop_pct <= 5
        results.append({
            "table": silver_tbl,
            "bronze_count": bronze_cnt,
            "silver_count": silver_cnt,
            "dropped": dropped,
            "drop_pct": drop_pct,
            "passed": passed
        })
        
        print_result(
            f"{silver_tbl}",
            passed,
            f"Bronze: {bronze_cnt:,} | Silver: {silver_cnt:,} | Dropped: {dropped:,} ({drop_pct:.2f}%)"
        )
    
    return results

# ============================================================
# 2. NULL CHECK VALIDATION
# ============================================================

def validate_null_checks():
    print_header("2. PRIMARY KEY NULL VALIDATION")
    
    pk_checks = [
        ("silver_name_basics", "nconst"),
        ("silver_title_basics", "tconst"),
        ("silver_title_ratings", "tconst"),
        ("silver_title_episode", "tconst"),
        ("silver_title_akas", "title_id"),
        ("silver_title_principals", "tconst"),
        ("silver_title_principals", "nconst"),
        ("silver_title_crew", "tconst"),
    ]
    
    for table, pk_col in pk_checks:
        null_count = spark.table(f"{SILVER_SCHEMA}.{table}").filter(col(pk_col).isNull()).count()
        passed = null_count == 0
        print_result(f"{table}.{pk_col}", passed, f"Null count: {null_count:,}")

# ============================================================
# 3. DATA TYPE VALIDATION
# ============================================================

def validate_data_types():
    print_header("3. DATA TYPE CASTING VALIDATION")
    
    # Check numeric columns are properly cast
    checks = [
        ("silver_name_basics", "birth_year", "int"),
        ("silver_name_basics", "death_year", "int"),
        ("silver_title_basics", "start_year", "int"),
        ("silver_title_basics", "end_year", "int"),
        ("silver_title_basics", "runtime_minutes", "int"),
        ("silver_title_basics", "is_adult", "boolean"),
        ("silver_title_ratings", "average_rating", "double"),
        ("silver_title_ratings", "num_votes", "int"),
        ("silver_title_episode", "season_number", "int"),
        ("silver_title_episode", "episode_number", "int"),
        ("silver_title_akas", "ordering", "int"),
        ("silver_title_akas", "is_original_title", "boolean"),
        ("silver_title_principals", "ordering", "int"),
    ]
    
    for table, column, expected_type in checks:
        df = spark.table(f"{SILVER_SCHEMA}.{table}")
        actual_type = dict(df.dtypes).get(column, "NOT_FOUND")
        passed = expected_type in actual_type.lower()
        print_result(f"{table}.{column}", passed, f"Expected: {expected_type} | Actual: {actual_type}")

# ============================================================
# 4. FORMAT VALIDATION (ID Patterns)
# ============================================================

def validate_id_formats():
    print_header("4. ID FORMAT VALIDATION")
    
    # nconst should match nm% pattern
    nm_invalid = spark.table(f"{SILVER_SCHEMA}.silver_name_basics") \
        .filter(~col("nconst").rlike("^nm[0-9]+$")).count()
    print_result("silver_name_basics.nconst (nm% pattern)", nm_invalid == 0, f"Invalid: {nm_invalid:,}")
    
    # tconst should match tt% pattern
    tt_tables = ["silver_title_basics", "silver_title_ratings", "silver_title_episode", 
                 "silver_title_principals", "silver_title_crew"]
    
    for table in tt_tables:
        tt_invalid = spark.table(f"{SILVER_SCHEMA}.{table}") \
            .filter(~col("tconst").rlike("^tt[0-9]+$")).count()
        print_result(f"{table}.tconst (tt% pattern)", tt_invalid == 0, f"Invalid: {tt_invalid:,}")
    
    # title_id in akas
    aka_invalid = spark.table(f"{SILVER_SCHEMA}.silver_title_akas") \
        .filter(~col("title_id").rlike("^tt[0-9]+$")).count()
    print_result("silver_title_akas.title_id (tt% pattern)", aka_invalid == 0, f"Invalid: {aka_invalid:,}")

# ============================================================
# 5. RANGE VALIDATION
# ============================================================

def validate_ranges():
    print_header("5. VALUE RANGE VALIDATION")
    
    # Birth/Death year validation (positive integers)
    df_names = spark.table(f"{SILVER_SCHEMA}.silver_name_basics")
    invalid_birth = df_names.filter((col("birth_year").isNotNull()) & (col("birth_year") <= 0)).count()
    invalid_death = df_names.filter((col("death_year").isNotNull()) & (col("death_year") <= 0)).count()
    print_result("birth_year > 0", invalid_birth == 0, f"Invalid: {invalid_birth:,}")
    print_result("death_year > 0", invalid_death == 0, f"Invalid: {invalid_death:,}")
    
    # Title year validation (1850-2035)
    df_titles = spark.table(f"{SILVER_SCHEMA}.silver_title_basics")
    invalid_start = df_titles.filter(
        (col("start_year").isNotNull()) & 
        ((col("start_year") < 1850) | (col("start_year") > 2035))
    ).count()
    invalid_end = df_titles.filter(
        (col("end_year").isNotNull()) & 
        ((col("end_year") < 1850) | (col("end_year") > 2035))
    ).count()
    print_result("start_year in [1850, 2035]", invalid_start == 0, f"Invalid: {invalid_start:,}")
    print_result("end_year in [1850, 2035]", invalid_end == 0, f"Invalid: {invalid_end:,}")
    
    # Rating validation (1.0-10.0)
    df_ratings = spark.table(f"{SILVER_SCHEMA}.silver_title_ratings")
    invalid_rating = df_ratings.filter(
        (col("average_rating") < 1.0) | (col("average_rating") > 10.0)
    ).count()
    print_result("average_rating in [1.0, 10.0]", invalid_rating == 0, f"Invalid: {invalid_rating:,}")
    
    # Votes validation (> 0)
    invalid_votes = df_ratings.filter(col("num_votes") <= 0).count()
    print_result("num_votes > 0", invalid_votes == 0, f"Invalid: {invalid_votes:,}")
    
    # Runtime validation (> 0)
    invalid_runtime = df_titles.filter(
        (col("runtime_minutes").isNotNull()) & (col("runtime_minutes") <= 0)
    ).count()
    print_result("runtime_minutes > 0", invalid_runtime == 0, f"Invalid: {invalid_runtime:,}")

# ============================================================
# 6. ARRAY TRANSFORMATION VALIDATION
# ============================================================

def validate_array_transforms():
    print_header("6. ARRAY TRANSFORMATION VALIDATION")
    
    # Check genres array is properly split
    df = spark.table(f"{SILVER_SCHEMA}.silver_title_basics")
    sample = df.filter(col("genres").isNotNull()).select("genres").first()
    if sample:
        is_array = isinstance(sample[0], list)
        print_result("genres is ARRAY type", is_array, f"Sample: {sample[0][:3] if is_array else sample[0]}...")
    
    # Check professions array
    df_names = spark.table(f"{SILVER_SCHEMA}.silver_name_basics")
    sample = df_names.filter(col("primary_professions").isNotNull()).select("primary_professions").first()
    if sample:
        is_array = isinstance(sample[0], list)
        print_result("primary_professions is ARRAY type", is_array, f"Sample: {sample[0][:3] if is_array else sample[0]}...")
    
    # Check directors/writers arrays
    df_crew = spark.table(f"{SILVER_SCHEMA}.silver_title_crew")
    sample = df_crew.filter(col("directors").isNotNull()).select("directors").first()
    if sample:
        is_array = isinstance(sample[0], list)
        print_result("directors is ARRAY type", is_array, f"Sample: {sample[0][:2] if is_array else sample[0]}...")

# ============================================================
# 7. ENUM VALIDATION (Title Types)
# ============================================================

def validate_enums():
    print_header("7. ENUM VALUE VALIDATION")
    
    VALID_TITLE_TYPES = [
        "movie", "short", "tvSeries", "tvEpisode", "tvMiniSeries", 
        "tvMovie", "tvSpecial", "tvShort", "video", "videoGame", "tvPilot"
    ]
    
    df = spark.table(f"{SILVER_SCHEMA}.silver_title_basics")
    invalid_types = df.filter(~col("title_type").isin(VALID_TITLE_TYPES)).count()
    print_result("title_type in VALID_TITLE_TYPES", invalid_types == 0, f"Invalid: {invalid_types:,}")
    
    # Show distribution
    print("\n  Title Type Distribution:")
    df.groupBy("title_type").count().orderBy(desc("count")).show(15, truncate=False)

# ============================================================
# 8. REFERENTIAL INTEGRITY (Episode → Parent)
# ============================================================

def validate_referential_integrity():
    print_header("8. REFERENTIAL INTEGRITY VALIDATION")
    
    # Episode tconst != parent_tconst
    df_ep = spark.table(f"{SILVER_SCHEMA}.silver_title_episode")
    self_ref = df_ep.filter(col("tconst") == col("parent_tconst")).count()
    print_result("episode tconst != parent_tconst", self_ref == 0, f"Self-references: {self_ref:,}")
    
    # Check parent exists in title_basics
    df_titles = spark.table(f"{SILVER_SCHEMA}.silver_title_basics")
    orphan_episodes = df_ep.join(
        df_titles, df_ep.parent_tconst == df_titles.tconst, "left_anti"
    ).count()
    print_result("parent_tconst exists in titles", orphan_episodes == 0, f"Orphans: {orphan_episodes:,}")

# ============================================================
# 9. DUPLICATE CHECK
# ============================================================

def validate_duplicates():
    print_header("9. DUPLICATE KEY VALIDATION")
    
    pk_checks = [
        ("silver_name_basics", ["nconst"]),
        ("silver_title_basics", ["tconst"]),
        ("silver_title_ratings", ["tconst"]),
        ("silver_title_crew", ["tconst"]),
        ("silver_title_akas", ["title_id", "ordering"]),
        ("silver_title_principals", ["tconst", "ordering"]),
    ]
    
    for table, pk_cols in pk_checks:
        df = spark.table(f"{SILVER_SCHEMA}.{table}")
        total = df.count()
        distinct = df.select(pk_cols).distinct().count()
        duplicates = total - distinct
        passed = duplicates == 0
        print_result(f"{table} ({', '.join(pk_cols)})", passed, f"Duplicates: {duplicates:,}")

# ============================================================
# 10. FRESHNESS CHECK
# ============================================================

def validate_freshness():
    print_header("10. DATA FRESHNESS VALIDATION")
    
    tables = [
        "silver_name_basics", "silver_title_basics", "silver_title_ratings",
        "silver_title_episode", "silver_title_akas", "silver_title_principals", "silver_title_crew"
    ]
    
    for table in tables:
        df = spark.table(f"{SILVER_SCHEMA}.{table}")
        latest = df.agg(max("silver_processed_at")).collect()[0][0]
        if latest:
            age_hours = (datetime.now() - latest.replace(tzinfo=None)).total_seconds() / 3600
            passed = age_hours < 24  # Data should be less than 24 hours old
            print_result(f"{table}", passed, f"Last processed: {latest} ({age_hours:.1f} hours ago)")
        else:
            print_result(f"{table}", False, "No processed timestamp found")

# ============================================================
# MAIN EXECUTION
# ============================================================

def run_all_qa_checks():
    print("\n" + "="*60)
    print(" IMDb BRONZE → SILVER QA VALIDATION REPORT")
    print(f" Run Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("="*60)
    
    validate_row_counts()
    validate_null_checks()
    validate_data_types()
    validate_id_formats()
    validate_ranges()
    validate_array_transforms()
    validate_enums()
    validate_referential_integrity()
    validate_duplicates()
    validate_freshness()
    
    print("\n" + "="*60)
    print(" QA VALIDATION COMPLETE")
    print("="*60)

# Run the QA checks
run_all_qa_checks()