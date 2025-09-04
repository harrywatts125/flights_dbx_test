# import and register the datasource
from pyspark_datasources import OpenSkyDataSource
from pyspark.sql.functions import expr
spark.dataSource.register(OpenSkyDataSource)

rules = {
    "icao24_not_null": "icao24 IS NOT NULL",
    "coord_exist": "latitude IS NOT NULL AND longitude IS NOT NULL",
    "altitude_positive": "geo_altitude >= 0",
    "velocity_positive": "velocity >= 0"
}

quarantine_rules = "NOT({0})".format(" AND ".join(rules.values()))


# declare a streaming table
@dlt.view
def raw_ingest_flights():
    return spark.readStream.format("opensky").load()

@dlt.table(
  temporary=True,
  partition_cols=["is_quarantined"],
)
@dlt.expect_all(rules)
def flights_data_quarantine():
  return (
    spark.readStream.table("live.raw_ingest_flights").withColumn("is_quarantined", expr(quarantine_rules))
  )

@dlt.table
def ingest_flights_clean():
  return spark.read.table("live.flights_data_quarantine").filter("is_quarantined=false")

@dlt.table
def invalid_ingest_flights():
  return spark.read.table("live.flights_data_quarantine").filter("is_quarantined=true OR is_quarantined IS NULL")