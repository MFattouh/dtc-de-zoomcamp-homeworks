# %%
import duckdb
import pandas as pd

conn = duckdb.connect(database=':memory:')
conn.execute("CREATE TABLE trip AS SELECT * FROM read_parquet('green_tripdata_2025-11.parquet')")
conn.execute("CREATE TABLE zone AS SELECT * FROM read_csv_auto('taxi_zone_lookup.csv')")

# %% [markdown]
# ## Question 3. Counting short trips
# For the trips in November 2025 (lpep_pickup_datetime between '2025-11-01' and '2025-12-01', exclusive of the upper bound), how many trips had a trip_distance of less than or equal to 1 mile?
# 

# %%
result_q3 = conn.execute("""
    SELECT COUNT(*) as trip_count
    FROM trip
    WHERE lpep_pickup_datetime >= '2025-11-01' 
        AND lpep_pickup_datetime < '2025-12-01'
        AND trip_distance <= 1
""").fetchall()

print("Q3: Number of trips in November 2025 with distance <= 1 mile: ", result_q3[0][0])

# %% [markdown]
# ## Question 4. Longest trip for each day
# Which was the pick up day with the longest trip distance? Only consider trips with trip_distance less than 100 miles (to exclude data errors).

# %%
result_q4 = conn.execute("""
    SELECT DATE(lpep_pickup_datetime) as pickup_date
    FROM trip
    WHERE trip_distance < 100
    ORDER BY trip_distance DESC
    LIMIT 1
""").fetchall()

print("Q4: Pick up day with the longest trip distance: ", result_q4[0][0])

# %% [markdown]
# ## Question 5. Biggest pickup zone
# Which was the pickup zone with the largest total_amount (sum of all trips) on November 18th, 2025?

# %%
result_q5 = conn.execute("""
    SELECT z.Zone
    FROM trip t
    JOIN zone z ON t.PULocationID = z.LocationID
    WHERE DATE(t.lpep_pickup_datetime) = '2025-11-18'
    GROUP BY z.Zone
    ORDER BY SUM(t.total_amount) DESC
    LIMIT 1
""").fetchall()

print("Q5: Zone with highest total amount on November 18, 2025: ", result_q5[0][0])

# %% [markdown]
# ## Question 6. Largest tip
# For the passengers picked up in the zone named "East Harlem North" in November 2025, which was the drop off zone that had the largest tip?

# %%
result_q6 = conn.execute("""
    SELECT z_dropoff.Zone
    FROM trip t
    JOIN zone z_pickup ON t.PULocationID = z_pickup.LocationID
    JOIN zone z_dropoff ON t.DOLocationID = z_dropoff.LocationID
    WHERE z_pickup.Zone = 'East Harlem North'
        AND t.lpep_pickup_datetime >= '2025-11-01'
        AND t.lpep_pickup_datetime < '2025-12-01'
    ORDER BY t.tip_amount DESC
    LIMIT 1
""").fetchall()

print("Q6: Drop off zone with largest tip for East Harlem North pickups in November 2025: ", result_q6[0][0] if result_q6 else "No data")