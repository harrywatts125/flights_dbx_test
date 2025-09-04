import dlt
import json
from pyspark.sql.functions import col, expr, array, lit, when

def create_dlt_pipeline(input_table_name: str, output_table_name: str, rules_json_string: str):
    """
    A factory that creates a validated silver table and a quarantine table from an
    unvalidated, user-transformed input table.

    Args:
        input_table_name: The name of the upstream table containing the custom transformations (e.g., "silver_customer_data_unvalidated").
        output_table_name: The desired name for the final, validated silver table.
        rules_json_string: A string containing the JSON rules configuration.
    """
    # --- 1. Load Rules with Friendly Names ---
    rules_dict = json.loads(rules_json_string)["rules"]
    categorized_rules = {
        "drop": {r.get("friendly_name", r["name"]): r["constraint"] for r in rules_dict if r["action"] == "drop"},
        "warn": {r.get("friendly_name", r["name"]): r["constraint"] for r in rules_dict if r["action"] == "warn"},
        "fail": {r.get("friendly_name", r["name"]): r["constraint"] for r in rules_dict if r["action"] == "fail"}
    }
    
    # --- 2. Define Final, Validated Silver Table ---
    # This is the single point of truth for quality. All rules are applied here.
    @dlt.table(name=output_table_name)
    @dlt.expect_all_or_drop(categorized_rules["drop"])
    @dlt.expect_all(categorized_rules["warn"])
    @dlt.expect_all_or_fail(categorized_rules["fail"])
    def validated_silver():
        return dlt.read_stream(input_table_name)

    # --- 3. Define the Quarantine Logic ---
    # This requires re-evaluating the rules to capture all failures.
    
    # We create these functions locally to encapsulate the logic
    # and register them with unique names to avoid DLT clashes.
    
    quarantine_name = f"{output_table_name}_quarantine"
    intermediate_name = f"{quarantine_name}_intermediate_checks"

    @dlt.table(name=intermediate_name, temporary=True)
    def intermediate_checks_for_quarantine():
        source_df = dlt.read_stream(input_table_name)
        for r in rules_dict:
            source_df = source_df.withColumn(f"_rule_passed_{r['name']}", expr(r['constraint']))
        
        failed_rules_array_expr = array(*[when(col(f"_rule_passed_{r['name']}") == False, lit(r.get("friendly_name", r["name"]))) for r in rules_dict])
        df_unfiltered = source_df.withColumn("failed_rules_unfiltered", failed_rules_array_expr)
        
        return df_unfiltered.withColumn("failed_rules", expr("filter(failed_rules_unfiltered, x -> x is not null)"))

    @dlt.table(name=quarantine_name)
    def quarantine():
        all_rule_names = [r["name"] for r in rules_dict]
        any_rule_failed_condition = " OR ".join([f"NOT _rule_passed_{name}" for name in all_rule_names])
        
        original_cols = dlt.read(input_table_name).columns
        final_cols = original_cols + ["failed_rules"]

        return dlt.read_stream(intermediate_name).filter(any_rule_failed_condition).select(*final_cols)

    # Register the dynamically created quarantine functions
    globals()[f"{intermediate_name}_func"] = intermediate_checks_for_quarantine
    globals()[f"{quarantine_name}_func"] = quarantine
    globals()[f"{output_table_name}_func"] = validated_silver