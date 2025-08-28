# import and register the datasource
from pyspark_datasources import OpenSkyDataSource
spark.dataSource.register(OpenSkyDataSource)

# declare a streaming table
@dlt.table
def ingest_flights():
    return spark.readStream.format("opensky").load()