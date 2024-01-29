# Sparkify-Data-Lake

## 1. Background
Sparkify, a rapidly growing music streaming startup, is planning to transfer its user and song databases from a data warehouse to a data lake, in response to its swiftly increasing data volume. To achieve this, our task is to construct an ETL (Extract, Transform, Load) pipeline. This pipeline will be designed to extract both databases from S3, process them using Spark, and then reload the data into S3, formatted as a collection of dimensional tables."

## 2. Project Datasets
Current user activity log data and song data resides in S3. The user activity log is a directory of JSON logs  and song data is a directory with JSON metadata.

1) Song Dataset
   
Each file is in JSON format and contains metadata about a song and the artist of that song. The files are partitioned by the first three letters of each song's track ID. Example:
   * song_data/A/B/C/TRABCEI128F424C983.json
   * song_data/A/A/B/TRAABJL12903CDCF1A.json
     
    Below is an example of what a single song file, TRAABJL12903CDCF1A.json, looks like.

    {"num_songs": 1, "artist_id": "ARJIE2Y1187B994AB7", "artist_latitude": null, "artist_longitude": null, "artist_location": "", "artist_name": "Line Renaud", "song_id": "SOUPIRU12A6D4FA1E1", "title": "Der Kleine Dompfaff", "duration": 152.92036, "year": 0}

2) Log Dataset
   
The log files in the dataset you'll be working with are partitioned by year and month. Example:
   * log_data/2018/11/2018-11-12-events.json
   * log_data/2018/11/2018-11-13-events.json
   below is an example of what the data in a log file, 2018-11-12-events.json, looks like.

   ![log-data](https://github.com/JOHNNIENYC/Sparkify-Data-Lake/assets/108529441/80512c0e-45ac-43f4-b61e-d4c3fcdd25c0)

## 3. Schema design
The project will create a star schema for queries on song play analysis, including below tables: 
#### Fact table
1) songplays
   * songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent
#### Dimension Tables
2) users
   * user_id, first_name, last_name, gender, level
3) songs
   * song_id, title, artist_id, year, duration
4) artists
   * artist_id, name, location, lattitude, longitude
5) time
   * start_time, hour, day, week, month, year, weekday
  
## 4. ETL pipeline

