# DE-ZoomCamp-Module1-Docker-SQL
Model 1 - DE ZoomCamp - Exercises 

Run docker with the python:3.13 image. Use an entrypoint bash to interact with the container.

What's the version of pip in the image?

- 25.3

Given the following docker-compose.yaml, what is the hostname and port that pgadmin should use to connect to the postgres database?


- postgres:5432

- db:5432


For the trips in November 2025 (lpep_pickup_datetime between '2025-11-01' and '2025-12-01', exclusive of the upper bound), how many trips had a trip_distance of less than or equal to 1 mile?

SELECT COUNT(1) FROM yellow_taxi_data WHERE lpep_pickup_datetime >= '2025-11-01' AND 
 lpep_pickup_datetime < '2025-12-01' AND trip_distance <=1

 8007
