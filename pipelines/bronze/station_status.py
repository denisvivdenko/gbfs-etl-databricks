from pyspark import pipelines as dp
from pyspark.sql.functions import col, current_timestamp, regexp_extract

SOURCE = "/Volumes/gbfs/bronze/raw_data"


@dp.table(
    name="bronze_station_status",
    comment="Raw station status, landed as-is",
    table_properties={"quality": "bronze"}
)
def bronze_station_status():
    return (
        spark.readStream
            .format("cloudFiles")
            .option("cloudFiles.format", "json")
            .option("cloudFiles.inferColumnTypes", "true")
            .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
            .option("rescuedDataColumn", "_rescued_data")
            .load(f"{SOURCE}/BoltEU_Brussels/station_status/")
            .select("*",
                    col("_metadata.file_path").alias("_source_file"),
                    col("_metadata.file_modification_time").alias("_file_modified_at"),
                    current_timestamp().alias("_ingested_at"),
                    regexp_extract(col("_metadata.file_path"),
                                   r"/([^/]+)/station_status/", 1).alias("_provider"))
    )
