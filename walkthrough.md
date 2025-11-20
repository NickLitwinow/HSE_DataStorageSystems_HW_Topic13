# Spark Data Processing HW Walkthrough

## Overview
Successfully completed the "Data Processing in Data Lake" homework assignment. The solution involves a PySpark script that processes NYC Yellow Taxi data from Jan-June 2025, cleans it, joins it with zone data, and calculates the average hourly orders per pickup zone.

## Changes
### Scripts
- **[hw_spark.py](file:///Users/litwein/GithubControl/HSE_DataStorageSystems_HW_Topic13/hw_spark.py)**: Main processing script.
    - Loads parquet data for Jan-Jun 2025.
    - Filters invalid records (dates, distance, passengers).
    - Adds hour columns.
    - Joins with `taxi_zone_lookup.csv`.
    - Aggregates and pivots data to show average orders per zone per hour.
    - Saves result to `result.parquet`.
- **[download_data.sh](file:///Users/litwein/GithubControl/HSE_DataStorageSystems_HW_Topic13/download_data.sh)**: Helper script to download data from the official TLC source.
- **[setup_java.sh](file:///Users/litwein/GithubControl/HSE_DataStorageSystems_HW_Topic13/setup_java.sh)**: Helper to install a Spark-compatible Java version (JDK 21).

### Environment
- Created a virtual environment `venv` with `pyspark`, `pandas`, and `pyarrow`.
- Installed OpenJDK 21 locally to resolve compatibility issues with the system's Java 25.

## Verification Results

### Automated Verification
Ran `verify_result.py` to check the output schema and content.

**Output Summary:**
- **Row Count**: 260 (One per zone)
- **Column Count**: 25 (Pickup_Zone + Hours 0-23)
- **Schema Check**: PASSED

**Sample Output:**
```text
+--------------------+------------------+----+---+------------------+------------------+
|         Pickup_Zone|                 0|   1|  2|                 3|                 4| ...
+--------------------+------------------+----+---+------------------+------------------+
|           Homecrest|               1.0| 1.5|1.5|               1.0|               1.0| ...
|Governor's Island...|               0.0| 0.0|0.0|               0.0|               0.0| ...
|              Corona|1.0454545454545454| 1.2|1.0|1.3636363636363635|1.1071428571428572| ...
+--------------------+------------------+----+---+------------------+------------------+
```

## How to Run
1.  **Setup Environment**:
    ```bash
    python3 -m venv venv
    source venv/bin/activate
    pip install pyspark pandas pyarrow
    ```
2.  **Setup Java** (if needed):
    ```bash
    ./setup_java.sh
    export JAVA_HOME=$(pwd)/jdk_local/jdk-21.0.2.jdk/Contents/Home
    ```
3.  **Download Data**:
    ```bash
    ./download_data.sh
    ```
4.  **Run Processing**:
    ```bash
    python3 hw_spark.py
    ```
