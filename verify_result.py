import os
from pyspark.sql import SparkSession

def verify():
    spark = SparkSession.builder.appName("Verify").getOrCreate()
    df = spark.read.parquet("result.parquet")
    
    print("Schema:")
    df.printSchema()
    
    print(f"Row count: {df.count()}")
    print(f"Column count: {len(df.columns)}")
    
    # Verify columns: Pickup_Zone + 0..23
    expected_cols = {"Pickup_Zone"} | {str(i) for i in range(24)}
    actual_cols = set(df.columns)
    
    missing = expected_cols - actual_cols
    extra = actual_cols - expected_cols
    
    if not missing:
        print("Verification PASSED: All expected columns are present.")
    else:
        print(f"Verification FAILED: Missing columns: {missing}")
        
    df.show(5)

if __name__ == "__main__":
    verify()
