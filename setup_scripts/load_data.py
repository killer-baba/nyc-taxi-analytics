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

print("\nAll files downloaded.")
