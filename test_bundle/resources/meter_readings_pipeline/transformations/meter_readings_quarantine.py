import dlt
from pyspark.sql.functions import col, sha2, concat_ws

# Import the factory function from utilities module
from utilities.pipeline_factory import create_dlt_pipeline

# ------------------------------------------------------------------------------------
# Step 1 : Define Bronze tables

@dlt.table(name="bronze_meter_readings", comment="Raw meter readings data.")
def bronze_meter_readings():
    return spark.readStream.format("cloudFiles").option("cloudFiles.format", "json").load("/Volumes/loomsight_catalog/meter_readings_dlt/meter_readings_vol/meter_readings/")

@dlt.table(name="bronze_customer_data", comment="Raw customer data.")
def bronze_customer_data():
    return spark.readStream.format("cloudFiles").option("cloudFiles.format", "json").load("/Volumes/loomsight_catalog/meter_readings_dlt/meter_readings_vol/customer_data/")

# Step 2: Define the "Unvalidated" Silver Tables with Custom Business Logic

# --- Example using Python (PySpark) ---
@dlt.table(name="silver_meter_readings_unvalidated")
def silver_meter_readings_unvalidated():
    return dlt.read_stream("bronze_meter_readings").select(
        col("meter_id"),
        col("reading_timestamp"),
        col("meter_reading_kwh").alias("kwh"), # Business logic: rename column
        col("customer_id")
    )

# --- Example using SQL ---
@dlt.table(name="silver_customer_data_unvalidated")
def silver_customer_data_unvalidated():
    return spark.sql("""
        SELECT
            sha2(concat_ws('||', customer_id, email), 256) as customer_pk, -- Business logic: create PK
            concat_ws(' ', first_name, last_name) as full_name,           -- Business logic: combine name
            email,
            phone
        FROM STREAM(LIVE.bronze_customer_data)
    """)

# ------------------------------------------------------------------------------------
# Step 3: Define Rules and Call the Factory to Create Final, Validated Tables

# --- METER READINGS ---
meter_rules_string = """
{ "rules": [
    {"name": "valid_reading", "friendly_name": "KWH reading must be positive", "constraint": "kwh >= 0", "action": "drop"},
    {"name": "valid_timestamp", "friendly_name": "Timestamp cannot be in the future", "constraint": "reading_timestamp <= current_timestamp()", "action": "warn"}
]}"""

create_dlt_pipeline(
    input_table_name="silver_meter_readings_unvalidated",
    output_table_name="silver_meter_readings",
    rules_json_string=meter_rules_string
)

# --- CUSTOMER DATA ---
customer_rules_string = """
{ "rules": [
    {"name": "valid_email", "friendly_name": "Email format must be valid", "constraint": "email LIKE '%@%.%'", "action": "drop"},
    {"name": "full_name_not_empty", "friendly_name": "Full name must not be empty", "constraint": "full_name IS NOT NULL AND full_name != ' '", "action": "warn"}
]}"""

create_dlt_pipeline(
    input_table_name="silver_customer_data_unvalidated",
    output_table_name="silver_customer_data",
    rules_json_string=customer_rules_string
)