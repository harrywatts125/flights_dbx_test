# import and register the datasource
from pyspark_datasources import OpenSkyDataSource
spark.dataSource.register(OpenSkyDataSource)

@dlt.expect("icao24_not_null", "icao24 IS NOT NULL")
@dlt.expect_or_drop("coord_exist", "latitude IS NOT NULL AND longitude IS NOT NULL")
@dlt.expect_or_drop("altitude_positive", "geo_altitude >= 0")
@dlt.expect_or_drop("velocity_positive", "velocity >= 0")

# declare a streaming table
@dlt.table
def ingest_flights():
    return spark.readStream.format("opensky").load()