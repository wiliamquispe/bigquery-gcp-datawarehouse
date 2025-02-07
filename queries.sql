-- Creating external
CREATE OR REPLACE EXTERNAL TABLE `de-bigquery-w3.yellow_2024_dataset.external_yellow_tripdata`
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://de-bigquery-w3-gcs/yellow_tripdata_2024-01.parquet', 'gs://de-bigquery-w3-gcs/yellow_tripdata_2024-02.parquet', 'gs://de-bigquery-w3-gcs/yellow_tripdata_2024-03.parquet', 'gs://de-bigquery-w3-gcs/yellow_tripdata_2024-04.parquet', 'gs://de-bigquery-w3-gcs/yellow_tripdata_2024-05.parquet', 'gs://de-bigquery-w3-gcs/yellow_tripdata_2024-06.parquet']
);

-- Create a materialized non partitioned table from external table
CREATE OR REPLACE TABLE de-bigquery-w3.yellow_2024_dataset.yellow_tripdata_non_partitoned AS
SELECT * FROM de-bigquery-w3.yellow_2024_dataset.external_yellow_tripdata;

-- Create a partitioned table by tpep_dropoff_datetime and cluster by VendorID from external table
CREATE OR REPLACE TABLE de-bigquery-w3.yellow_2024_dataset.yellow_tripdata_partitoned_clustered
PARTITION BY DATE(tpep_dropoff_datetime)
CLUSTER BY VendorID AS
SELECT * FROM de-bigquery-w3.yellow_2024_dataset.external_yellow_tripdata;

-- Distict values of PULocationIDs on external and normal table 
SELECT COUNT(DISTINCT PULocationID) FROM de-bigquery-w3.yellow_2024_dataset.external_yellow_tripdata;
SELECT COUNT(DISTINCT PULocationID) FROM de-bigquery-w3.yellow_2024_dataset.yellow_tripdata_non_partitoned;

-- Retrieve specific columns from materialized table
SELECT PULocationID FROM de-bigquery-w3.yellow_2024_dataset.yellow_tripdata_non_partitoned;
SELECT PULocationID, DOLocationID FROM de-bigquery-w3.yellow_2024_dataset.yellow_tripdata_non_partitoned;

SELECT COUNT(1) FROM de-bigquery-w3.yellow_2024_dataset.yellow_tripdata_non_partitoned WHERE fare_amount = 0;

SELECT DISTINCT VendorID FROM de-bigquery-w3.yellow_2024_dataset.yellow_tripdata_non_partitoned WHERE DATE(tpep_dropoff_datetime) > '2024-03-01' AND DATE(tpep_dropoff_datetime) <= '2024-03-15';

SELECT DISTINCT VendorID FROM de-bigquery-w3.yellow_2024_dataset.yellow_tripdata_partitoned_clustered WHERE DATE(tpep_dropoff_datetime) > '2024-03-01' AND DATE(tpep_dropoff_datetime) <= '2024-03-15';

-- Count the number of rows in the table
SELECT count(*) FROM de-bigquery-w3.yellow_2024_dataset.yellow_tripdata_non_partitoned