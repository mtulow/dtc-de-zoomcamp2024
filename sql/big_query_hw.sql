/** 
  Homework 03
SETUP:
- Create an external table using the Green
          Taxi Trip Records Data for 2022.
- Create a table in BQ using the Green Taxi Trip Records
      for 2022 (do not partition or cluster this table).
 **/

-- Creating external table of all green taxi trips in 2022
CREATE OR REPLACE EXTERNAL TABLE `ny_taxi.external_green_tripdata`
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://nyc-tlc-trips/green_tripdata-2022-*.parquet']
);
-- Create a non partitioned table from external table
CREATE OR REPLACE TABLE `ny_taxi.green_tripdata_non_partitioned` AS
SELECT
  vendor_id,
  cast(pickup_datetime as timestamp) as pickup_datetime,
  cast(dropoff_datetime as timestamp) as dropoff_datetime,
  store_and_fwd_flag,
  ratecode_id,
  pu_location_id,
  do_location_id,
  passenger_count,
  trip_distance,
  fare_amount,
  extra,
  mta_tax,
  tip_amount,
  tolls_amount,
  ehail_fee,
  improvement_surcharge,
  total_amount,
  payment_type,
  trip_type,
  congestion_surcharge
FROM
  `ny_taxi.external_green_tripdata`;


/** 
  Questions:
**/
-- Question 1: What is count of records for the 2022 Green Taxi Data?
SELECT
    count(1)
FROM
    `ny_taxi.green_tripdata_non_partitioned`;
-- Solution: 840,402

/**
  Question 2:
  Write a query to count the distinct number of PULocationIDs 
    for the entire dataset on both the tables.
  What is the estimated amount of data that will be read when
    this query is executed on the External Table and the Table?
**/
-- External table[
SELECT
  COUNT(DISTINCT pu_location_id) AS distinct_pu_location_ids
FROM
  `ny_taxi.external_green_tripdata`;
-- Solution: ...

-- Non-partitioned table
SELECT
  COUNT(DISTINCT pu_location_id) AS distinct_pu_location_ids
FROM
  `ny_taxi.green_tripdata_non_partitioned`;
-- Solution: ...


/**
    Question 3:
  How many records have a fare_amount of 0?
**/
SELECT
  count(*)
FROM
  `ny_taxi.external_green_tripdata`
WHERE
  fare_amount = 0;
-- Solution: ...


/**
  Question 4:
    What is the best strategy to make an optimized table in Big Query if your
    query will always order the results by PUlocationID and filter based on
    lpep_pickup_datetime? (Create a new table with this strategy)
**/
-- Create a partitioned table from external table
CREATE OR REPLACE TABLE `ny_taxi.green_tripdata_partitioned_cluster`
PARTITION BY
  pickup_date
CLUSTER BY
  pu_location_id AS
SELECT 
  vendor_id,
  cast(pickup_datetime as timestamp) as pickup_datetime,
  cast(dropoff_datetime as timestamp) as dropoff_datetime,
  store_and_fwd_flag,
  ratecode_id,
  pu_location_id,
  do_location_id,
  passenger_count,
  trip_distance,
  fare_amount,
  extra,
  mta_tax,
  tip_amount,
  tolls_amount,
  ehail_fee,
  improvement_surcharge,
  total_amount,
  payment_type,
  trip_type,
  congestion_surcharge
FROM
  `ny_taxi.external_green_tripdata`;
-- Solution: partition by pickup_datetime Cluster on pu_location_id


/**
  Question 5:
    Write a query to retrieve the distinct `pu_location_id` between 
    `pickup_datetime` 06/01/2022 and 06/30/2022 (inclusive)
**/
SELECT
    DISTINCT pu_location_id,
FROM
    `ny_taxi.green_tripdata_non_partitioned`
WHERE
    pickup_datetime BETWEEN '2022-06-01' AND '2022-06-30';
-- 
SELECT
    DISTINCT pu_location_id,
FROM
    `ny_taxi.green_tripdata_partitioned`
WHERE
    pickup_datetime BETWEEN '2022-06-01' AND '2022-06-30';
-- Solution: Non-partitioned = 12.82 MB ; partitioned = 1.12 MB


/** 
    Question 6:
  Where is the data stored in the External Table you created?
**/
-- Solution: GCP Storage Bucket



/** 
    Question 7:
  It is best practice in Big Query to always cluster your data:
**/
-- Solution: No

/** 
    (BONUS: Not worth points) Question 8:
  Write a SELECT count(*) query FROM the materialized table you created. How many bytes does it estimate will be read? Why?
**/

-- Solution: ...

