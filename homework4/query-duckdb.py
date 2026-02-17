# %%
import duckdb

conn = duckdb.connect(database='taxi_rides_ny/taxi_rides_ny.duckdb', read_only=True)
# %% [markdown]
### Question 3. Counting Records in `fct_monthly_zone_revenue`
# After running your dbt project, query the `fct_monthly_zone_revenue` model.
# What is the count of records in the `fct_monthly_zone_revenue` model?
# %%
result_q3 = conn.query("""
    SELECT COUNT(*) 
    FROM taxi_rides_ny.prod.fct_monthly_zone_revenue
""").fetchall()
print(result_q3)

# %% [markdown]
### Question 4. Best Performing Zone for Green Taxis (2020)
# Using the `fct_monthly_zone_revenue` table, find the pickup zone with the **highest total revenue** (`revenue_monthly_total_amount`) for **Green** taxi trips in 2020.
# Which zone had the highest revenue?
# %%
conn.query("""
    SELECT *
    FROM taxi_rides_ny.prod.fct_monthly_zone_revenue
    LIMIT 10
""").fetchall()
# %%
result_q4 = conn.query("""
    SELECT pickup_zone
    FROM taxi_rides_ny.prod.fct_monthly_zone_revenue
    WHERE service_type = 'Green' AND EXTRACT(YEAR FROM revenue_month) = 2020
    ORDER BY revenue_monthly_total_amount DESC
    LIMIT 1
""").fetchall()
print(result_q4)

# %% [markdown]
### Question 5. Green Taxi Trip Counts (October 2019)
# Using the `fct_monthly_zone_revenue` table, what is the **total number of trips** (`total_monthly_trips`) for Green taxis in October 2019?

# %%
result_q5 = conn.query("""
    SELECT sum(total_monthly_trips)
    FROM taxi_rides_ny.prod.fct_monthly_zone_revenue
    GROUP BY service_type, revenue_month
    HAVING service_type = 'Green' AND revenue_month = '2019-10-01'
""").fetchall()
print(result_q5)

# %% [markdown]
### Question 6. Build a Staging Model for FHV Data

#Create a staging model for the **For-Hire Vehicle (FHV)** trip data for 2019.

# 1. Load the [FHV trip data for 2019](https://github.com/DataTalksClub/nyc-tlc-data/releases/tag/fhv) into your data warehouse
# 2. Create a staging model `stg_fhv_tripdata` with these requirements:
#   - Filter out records where `dispatching_base_num IS NULL`
#   - Rename fields to match your project's naming conventions (e.g., `PUlocationID` → `pickup_location_id`)

# What is the count of records in `stg_fhv_tripdata`?

# %%
result_q6 = conn.query("""
    SELECT COUNT(*) 
    FROM taxi_rides_ny.prod.stg_fhv_tripdata
""").fetchall()
print(result_q6)