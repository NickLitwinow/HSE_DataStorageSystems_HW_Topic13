# Spark Data Processing HW Implementation Plan

## Goal Description
Complete the "Data Processing in Data Lake" homework assignment using Apache Spark. The goal is to process NYC Yellow Taxi data for the first half of 2025, clean it, join with zone data, and calculate average hourly orders per zone.

## Proposed Changes

### Project Root
#### [NEW] [hw_spark.py](file:///Users/litwein/GithubControl/HSE_DataStorageSystems_HW_Topic13/hw_spark.py)
- Main Python script using PySpark.
- **Steps**:
    1.  **Download**: Helper function to download files if not present.
    2.  **Load**: Read parquet files into a Spark DataFrame.
    3.  **Clean**: Filter based on:
        - `tpep_pickup_datetime` and `tpep_dropoff_datetime` within 01.01.2025 - 30.06.2025.
        - `trip_distance` > 0.
        - `passenger_count` > 0.
    4.  **Transform**:
        - Extract hour from pickup and dropoff times.
        - Select columns: `tpep_pickup_datetime`, `tpep_dropoff_datetime`, `passenger_count`, `trip_distance`, `PULocationID`, `DOLocationID`, `total_amount`, `pickup_hour`, `dropoff_hour`.
    5.  **Join**:
        - Load `taxi_zone_lookup.csv`.
        - Join to replace `PULocationID` and `DOLocationID` with Zone names.
    6.  **Aggregate**:
        - Group by Pickup Zone and Pickup Hour.
        - Count orders.
    7.  **Pivot**:
        - Calculate average orders per zone per hour.
        - Pivot table: Rows = Zone, Cols = Hours (0-23), Values = Average orders.
    8.  **Save**: Write result to `result.parquet`.

#### [NEW] [download_data.sh](file:///Users/litwein/GithubControl/HSE_DataStorageSystems_HW_Topic13/download_data.sh)
- Shell script to download the required parquet and csv files to a `data/` directory to keep the workspace clean.

## Verification Plan

### Automated Tests
- Run `python hw_spark.py`.
- Check if `result.parquet` is generated.
- Inspect the schema and first few rows of `result.parquet` to ensure it matches requirements (25 columns: Zone + 0-23 hours).

### Manual Verification
- Review the code against the checklist provided in the assignment.
