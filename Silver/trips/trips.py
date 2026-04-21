from pyspark import pipelines as dp
from pyspark.sql import functions as F

# Create a view and let's call it staging view
@dp.temporary_view(
    name="trips_silver_staging",
    comment="Transformed trips data ready for CDC upsert"
)
# Add validations
@dp.expect("valid_date", "year(business_date) >= 2020")
@dp.expect("valid_driver_rating", "driver_rating BETWEEN 1 AND 10")
@dp.expect("valid_passenger_rating", "passenger_rating BETWEEN 1 AND 10")
# Create a function which will read our bronze table
def trips_silver_staging():
    df_bronze = spark.readStream.table("transportation.bronze.trips")
    # When you use readStream, it will only read the updated records
    # because change data feed is enabled in the bronze function
    # "delta.enableChangeDataFeed": "true"

    # Renaming columns
    df_silver = df_bronze.select(
        F.col("trip_id").alias("id"),
        F.col("date").cast("date").alias("business_date"),
        F.col("city_id").alias("city_id"),
        F.col("passenger_type").alias("passenger_category"),
        F.col("distance_travelled_km").alias("distance_km"),
        F.col("fare_amount").alias("sales_amt"),
        F.col("passenger_rating").alias("passenger_rating"),
        F.col("driver_rating").alias("driver_rating"),
        F.col("ingest_datetime").alias("bronze_ingest_timestamp"),
    )

    # Add timestamp
    df_silver = df_silver.withColumn(
        "silver_processed_timestamp", F.current_timestamp()
    )

    return df_silver


# Create the silver table
dp.create_streaming_table(
    name="transportation.silver.trips",
    comment="Cleaned and validated orders with CDC upsert capability",
    table_properties={
        "quality": "silver",
        "layer": "silver",
        "delta.enableChangeDataFeed": "true",
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true"
    },
)

# Now, the next step will be to let your data flow from silver staging to transportation.silver.trips table
# And that you can do it using auto CDC flow
dp.create_auto_cdc_flow(
    target="transportation.silver.trips",
    source="trips_silver_staging",
    keys=["id"],
    sequence_by=F.col("silver_processed_timestamp"),
    stored_as_scd_type=1,
    except_column_list=[],
)

# What it will do is
# in the target table if there is a record with the same trip_id, it will update it
# and if there is no record, it will insert it
