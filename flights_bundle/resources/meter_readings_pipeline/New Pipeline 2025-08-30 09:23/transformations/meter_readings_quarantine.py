import dlt
import json
from pyspark.sql.functions import col, expr, array, lit, when

# --- Configuration ---
RULES_PATH = "/Volumes/loomsight_catalog/meter_readings_dlt/meter_readings_vol/rules.json"
SOURCE_PATH = "/Volumes/loomsight_catalog/meter_readings_dlt/meter_readings_vol/"

# --- Helper Function to Load Rules ---
def load_rules(path):
    """Loads and categorizes the data quality rules from the specified JSON file."""
    with open(path.replace("/dbfs", ""), "r") as f:
        rules = json.load(f)["rules"]
    return {
        "drop": {rule["name"]: rule["constraint"] for rule in rules if rule["action"] == "drop"},
        "warn": {rule["name"]: rule["constraint"] for rule in rules if rule["action"] == "warn"},
        "fail": {rule["name"]: rule["constraint"] for rule in rules if rule["action"] == "fail"}
    }

# Load and categorize the rules once
categorized_rules = load_rules(RULES_PATH)

# --- DLT Pipeline Definitions ---

# 1. BRONZE: Ingest raw data. (Unchanged)
@dlt.table(name="bronze_meter_readings")
def bronze_meter_readings():
    return (
        spark.readStream.format("cloudFiles")
            .option("cloudFiles.format", "json")
            .option("cloudFiles.schemaHints", "reading_timestamp timestamp")
            .load(SOURCE_PATH)
    )

# 2. INTERMEDIATE: Anrich the data with quality check results.
# This table is still essential for the quarantine logic.
@dlt.table(name="bronze_with_quality_checks", temporary=True)
def bronze_with_quality_checks():
    source_df = dlt.read_stream("bronze_meter_readings")
    all_rules = categorized_rules['drop'] | categorized_rules['warn'] | categorized_rules['fail']

    # Step 1: Add a boolean column for each rule's result
    for name, constraint in all_rules.items():
        source_df = source_df.withColumn(f"_rule_passed_{name}", expr(constraint))

    # Step 2: Create an array of failed rule names (with nulls)
    failed_rules_array_expr = array(
        *[
            when(col(f"_rule_passed_{name}") == False, lit(name))
            for name in all_rules.keys()
        ]
    )
    df_with_unfiltered_array = source_df.withColumn("failed_rules_unfiltered", failed_rules_array_expr)

    # Step 3: Filter the nulls from the array
    return df_with_unfiltered_array.withColumn(
        "failed_rules",
        expr("filter(failed_rules_unfiltered, x -> x is not null)")
    )

# 3. SILVER: The single point of truth for data quality enforcement and metrics.
# --- THIS IS THE KEY CHANGE ---
@dlt.table(name="silver_meter_readings")
@dlt.expect_all_or_drop(categorized_rules["drop"])   # Apply and enforce DROP rules
@dlt.expect_all(categorized_rules["warn"])          # Apply WARN rules (for metrics)
@dlt.expect_all_or_fail(categorized_rules["fail"])  # Apply and enforce FAIL rules
def silver_meter_readings():
    # This table now reads the ENTIRE enriched stream.
    # The decorators above handle all filtering and actions.
    # The DLT event log for this table will show metrics for ALL rules.
    
    # We only need to select the original columns for the final table.
    original_cols = dlt.read("bronze_meter_readings").columns
    return (
        dlt.read_stream("bronze_with_quality_checks")
            .select(*original_cols)
    )

# 4. QUARANTINE: Contains ALL rows that failed ANY rule. (Unchanged)
@dlt.table(name="quarantined_readings")
def quarantined_readings():
    all_rules = categorized_rules['drop'] | categorized_rules['warn'] | categorized_rules['fail']
    any_rule_failed_condition = " OR ".join([f"NOT _rule_passed_{name}" for name in all_rules.keys()])
    
    original_cols = dlt.read("bronze_meter_readings").columns
    final_cols = original_cols + ["failed_rules"]

    return (
        dlt.read_stream("bronze_with_quality_checks")
            .filter(any_rule_failed_condition)
            .select(*final_cols)
    )