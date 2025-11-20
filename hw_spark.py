import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, hour, avg, count

def main():
    # Initialize Spark Session
    spark = SparkSession.builder \
        .appName("NYC Taxi Data Processing") \
        .getOrCreate()

    # Define paths
    data_path = "data/yellow_tripdata_2025-*.parquet"
    zone_path = "data/taxi_zone_lookup.csv"
    output_path = "result.parquet"

    # 1. Load Data
    print("Loading trip data...")
    df = spark.read.parquet(data_path)

    # 2. Clean Data
    print("Cleaning data...")
    # Conditions:
    # - pickup time >= 2025-01-01
    # - pickup time <= 2025-06-30
    # - dropoff time >= 2025-01-01
    # - dropoff time <= 2025-06-30
    # - distance > 0
    # - passengers > 0
    
    # Note: Spark timestamps are usually comparable to strings or datetime objects.
    # We can use string comparison for simplicity or cast to timestamp.
    # Assuming standard format in parquet.
    
    df_cleaned = df.filter(
        (col("tpep_pickup_datetime") >= "2025-01-01") &
        (col("tpep_pickup_datetime") <= "2025-06-30 23:59:59") &
        (col("tpep_dropoff_datetime") >= "2025-01-01") &
        (col("tpep_dropoff_datetime") <= "2025-06-30 23:59:59") &
        (col("trip_distance") > 0) &
        (col("passenger_count") > 0)
    )

    # 3. Add Hour Columns
    print("Adding hour columns...")
    df_with_hours = df_cleaned.withColumn("pickup_hour", hour("tpep_pickup_datetime")) \
                              .withColumn("dropoff_hour", hour("tpep_dropoff_datetime"))

    # 4. Select Columns
    print("Selecting columns...")
    required_columns = [
        "tpep_pickup_datetime",
        "tpep_dropoff_datetime",
        "passenger_count",
        "trip_distance",
        "PULocationID",
        "DOLocationID",
        "total_amount",
        "pickup_hour",
        "dropoff_hour"
    ]
    df_selected = df_with_hours.select(required_columns)

    # 5. Load and Join Zone Data
    print("Loading and joining zone data...")
    zone_df = spark.read.option("header", "true").csv(zone_path)
    
    # Join for Pickup Zone
    df_joined = df_selected.join(
        zone_df.withColumnRenamed("LocationID", "PULocationID") \
               .withColumnRenamed("Zone", "Pickup_Zone") \
               .select("PULocationID", "Pickup_Zone"),
        on="PULocationID",
        how="left"
    )
    
    # Join for Dropoff Zone
    df_joined = df_joined.join(
        zone_df.withColumnRenamed("LocationID", "DOLocationID") \
               .withColumnRenamed("Zone", "Dropoff_Zone") \
               .select("DOLocationID", "Dropoff_Zone"),
        on="DOLocationID",
        how="left"
    )
    
    # Replace IDs with Zone names in the final selection if needed, 
    # but the requirement says "result in the set were names of pickup and dropoff zones INSTEAD of identifiers".
    # So we should drop IDs and keep Zones.
    
    # 6. Aggregate
    print("Aggregating...")
    # "Collect a dataset with hourly aggregation of the number of orders by pickup zone."
    # This seems to be an intermediate step or part of the final pivot.
    # The final requirement is "Average number of orders in each zone".
    # "On the intersection of rows and columns is the average number of orders in the zone per hour."
    # This phrasing "average number of orders in the zone per hour" is slightly ambiguous.
    # Does it mean:
    # A) Total orders in that zone for that hour / Number of days? (Average daily orders for that hour)
    # B) Just the count? (But it says "average").
    # Let's re-read: "9. Соберите набор данных со средним количеством заказов в каждой зоне... На пересечении строк и столбов находится среднее количество заказов в зоне в час."
    # If we have 6 months of data.
    # For Zone A at Hour 0, we have many days.
    # We should calculate the count of orders for Zone A at Hour 0 for EACH day, and then average those counts?
    # Or is it just the total count divided by total hours?
    # Usually "average number of orders per hour" implies: Total Orders / Total Days (for that specific hour slot).
    
    # Let's do:
    # 1. Group by Zone, Date, Hour -> Count (Orders per specific hour per day)
    # 2. Group by Zone, Hour -> Avg(Count)
    
    from pyspark.sql.functions import to_date
    
    df_daily_hourly = df_joined.withColumn("pickup_date", to_date("tpep_pickup_datetime")) \
                               .groupBy("Pickup_Zone", "pickup_date", "pickup_hour") \
                               .count() \
                               .withColumnRenamed("count", "hourly_orders")
                               
    df_avg_hourly = df_daily_hourly.groupBy("Pickup_Zone", "pickup_hour") \
                                   .agg(avg("hourly_orders").alias("avg_orders"))
    
    # 7. Pivot
    print("Pivoting...")
    # Rows: Zone
    # Cols: Hours 0-23
    # Values: avg_orders
    
    pivot_df = df_avg_hourly.groupBy("Pickup_Zone") \
                            .pivot("pickup_hour", list(range(24))) \
                            .sum("avg_orders") \
                            .na.fill(0)
                            
    # 8. Save
    print(f"Saving to {output_path}...")
    pivot_df.write.mode("overwrite").parquet(output_path)
    
    print("Done.")

if __name__ == "__main__":
    main()
