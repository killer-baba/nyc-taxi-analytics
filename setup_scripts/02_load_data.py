"""
Download NYC TLC Yellow Taxi 2023 Parquet files
and load them into Snowflake via PUT + COPY INTO.

This script is a one-time data loading utility, not part of the DBT pipeline.
"""

import os
import urllib.request

# Download directory
os.makedirs("data", exist_ok=True)

BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data"
MONTHS = range(1, 13)

for month in MONTHS:
    filename = f"yellow_tripdata_2023-{month:02d}.parquet"
    url = f"{BASE_URL}/{filename}"
    filepath = os.path.join("data", filename)
    
    if os.path.exists(filepath):
        print(f"Already exists: {filename}")
        continue
    
    print(f"Downloading: {filename}...")
    urllib.request.urlretrieve(url, filepath)
    print(f"Done: {filename}")

# Download zone lookup CSV
zone_file = os.path.join("data", "taxi_zone_lookup.csv")
if not os.path.exists(zone_file):
    print("Downloading: taxi_zone_lookup.csv...")
    urllib.request.urlretrieve(
        "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv",
        zone_file
    )
    print("Done: taxi_zone_lookup.csv")

print("\nAll files downloaded.")
