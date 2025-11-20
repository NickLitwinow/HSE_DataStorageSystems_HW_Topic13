#!/bin/bash

mkdir -p data

# Corrected URL: trip-data instead of tripdata
BASE_URL="https://d37ci6vzurychx.cloudfront.net/trip-data"
ZONE_URL="https://d37ci6vzurychx.cloudfront.net/misc/taxi+_zone_lookup.csv"

echo "Downloading Taxi Zone Lookup..."
curl -o data/taxi_zone_lookup.csv "$ZONE_URL"

# Explicitly list months to ensure zero-padding
for month in 01 02 03 04 05 06; do
    FILE="yellow_tripdata_2025-$month.parquet"
    URL="$BASE_URL/$FILE"
    echo "Downloading $FILE from $URL..."
    curl -o "data/$FILE" "$URL"
done

echo "Download complete."
