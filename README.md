## Use Case
Sensors that measure various environmental parameters are installed in a city to monitor air quality. 
These include particulate matter, carbon monoxide, ozone, sulphur dioxide, nitrogen dioxide and other pollutants. 
The measured values are obtained via an API from the German Federal Environment Agency and processed in a stream 
processing pipeline with Apache Spark Structured Streaming. 
The aim is to store the collected data efficiently and make it available in aggregated form.

## Project Structure
1. API service
The API service includes the retrieval, storage and simulation of sensor data. 
A script retrieves data from the Umweltbundesamt API once a day (or at a customizable time) using a cronjob. 
This data comes from various cities in Germany and is used to generate a simulated data stream.

- fetch_data.py: Retrieves sensor data and metadata, processes it and saves it as .csv files. Metadata is written to a PostgreSQL table.
- stream_simulation.py: Simulates a data stream by reading lines of the .csv file one after the other and saving them as JSON files. Older files (older than 6 minutes) are deleted regularly.
- main.py: Orchestrates the entire process.

The Docker container api_service (container name: api_service_container) executes this service. 


2. PostgreSQL database

The database is provided as a Docker container and contains three tables:
- airquality_metadata: Contains information on the pollutant components.
- airquality_raw: Stores the complete historical data set of the sensor measurements.
- airquality_aggregated: Contains aggregated values (min, max, average) for each pollutant component.

The database can be queried via the terminal with the following command:
docker exec -it postgres_db psql -U postgres -d airquality_sensor_data


3. Spark Streaming Job
The Spark streaming job processes the continuously incoming sensor data and stores it in the PostgreSQL database. 
There are two main processes: the storage of the raw data and the aggregation of the measured values.

First, the streaming job reads in the incoming JSON files line by line. 
Each new measurement is saved in the raw table as a complete historical data set. 
The data is saved in append mode so that new data is added continuously without overwriting existing entries.

In parallel, the incoming sensor data is aggregated in 4-minute time windows. 
The maximum, minimum and average measured values within this period are calculated for each pollutant component. 
These aggregated values are then written to the aggregated table. 


## Using the code
The following steps must be followed to execute the project:

- Install Docker: The entire environment runs in Docker containers. 
- Start Docker-Compose: docker-compose up --build or use Docker Desktop
- Adjust cronjob: If necessary, the cronjob in the API service can be adjusted to execute the API query at a different time.
- Use healthcheck or start streaming manually (see comments in docker-compose.yml)