CREATE EXTERNAL TABLE `homework3.external_yellow_tripdata`
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://{MY_BUCKET}/yellow_tripdata_2024-*.parquet']
);

CREATE OR REPLACE TABLE homework3.yellow_tripdata_non_partitioned AS
SELECT * FROM homework3.external_yellow_tripdata;

SELECT COUNT("ROW") FROM `homework3.yellow_tripdata_non_partitioned`;

SELECT COUNT(DISTINCT PULocationID) FROM `homework3.external_yellow_tripdata`;

SELECT COUNT(DISTINCT PULocationID) FROM `homework3.yellow_tripdata_non_partitioned`;

SELECT PULocationID FROM `homework3.yellow_tripdata_non_partitioned`;

SELECT PULocationID, DOLocationID FROM `homework3.yellow_tripdata_non_partitioned`;

SELECT 
  COUNT(CASE WHEN fare_amount = 0 THEN 1 END) AS zero_count
FROM `homework3.yellow_tripdata_non_partitioned`;

CREATE OR REPLACE TABLE homework3.yellow_tripdata_partitioned
  PARTITION BY DATE(tpep_dropoff_datetime)
  CLUSTER BY VendorID AS
SELECT * FROM homework3.external_yellow_tripdata;

SELECT DISTINCT VendorID FROM homework3.yellow_tripdata_non_partitioned
WHERE DATE(tpep_dropoff_datetime) between '2024-03-01' AND '2024-03-15';

SELECT DISTINCT VendorID FROM homework3.yellow_tripdata_partitioned
WHERE DATE(tpep_dropoff_datetime) between '2024-03-01' AND '2024-03-15'

SELECT count(*) FROM homework3.yellow_tripdata_non_partitioned;
