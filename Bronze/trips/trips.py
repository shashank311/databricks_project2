from pyspark import pipelines as dp
import pyspark.sql.functions as F

SOURCE_PATH = "s3://shashank-goodcabs/data-store/trips"

@dp.table(
    name="transportation.bronze.trips",
    comment="Streaming pipeline of raw trips data with Auto Loader",
    table_properties={
        "quality": "bronze",
        "layer": "bronze",
        "source_format": "csv",
        "delta.enableChangeDataFeed": "true",
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true",
    },
)
def orders_bronze():
    df = (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("cloudFiles.schemaEvolutionMode", "rescue")
        .option("cloudFiles.maxFilesPerTrigger", 100)
        .load(SOURCE_PATH)
    )
    #readStream will read the data in streaming fashion
    #and the benefit is that whenever new file is dropped, 
    #Because of this code, only the new file will be processed

    #This is called AUTOLOADER Feature in databricks

    #schemaEvolutionMode will make sure if let's say the schema is changed, , then it will not fail
    #It will create a new rescue column
    #inferColumnTypes means based on the data infer the column types
    #maxFilesPerTrigger means when you drop the new files, it will process only 100 files at a time
    #but if 105 files are dropped, then 100 files will be in one batch and 5 files will be in another batch

    #Rename the problematic column
    df = df.withColumnRenamed(
        "distance_travelled(km)",
        "distance_travelled_km"
    )
    #This bracket might create issues for us, that's why we renamed it to distance_travelled_km
    df = df.withColumn("file_name", F.col("_metadata.file_path")).withColumn("ingest_datetime", 
        F.current_timestamp())

    return df
