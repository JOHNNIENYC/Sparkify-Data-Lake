import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
import pyspark.sql.functions as F



config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    """
    The function is used to create a Sparksession object
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark

def process_song_data(spark, input_data, output_data):
    """
    The function is used to extract song data from S3, transforming data using Spark and export the data back into S3 
    Parameters:
    ---------------------
    spark: 
          Spark session used to process data 
    input_data:
               S3 folder path prefix where the origial song dataset resides
    output_data:
               S3 folder path where the dataset will be exported from Spark after data processing 
    Returns
    ---------------------
    This function will create two dimension tables in S3, include "songs" table and "artists" table
    
    """
    # get filepath to song data file
    song_data = input_data + "song_data/*/*/*"
    
    # read song data file
    
    df_song_data = spark.read.json(song_data)

    # extract columns to create songs table
    songs_table = df_song_data.select("song_id", "title", "artist_id", "year", "duration").dropDuplicates(subset=['song_id'])
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.mode("overwrite").partitionBy('year', 'artist_id').parquet(output_data + "songs.parquet")

    # extract columns to create artists table
    artists_table = df_song_data.select("artist_id", "artist_name", "artist_location", "artist_latitude", "artist_longitude").dropDuplicates(subset=['artist_id'])
    
    # write artists table to parquet files
    artists_table.write.mode("overwrite").parquet(output_data + "artists.parquet")


def process_log_data(spark, input_data, output_data):
    """
    The function is used to extract log data from S3, transforming data using Spark and export the data back into S3
    
    Parameters:
    ---------------------
    spark: 
          Spark session used to process data 
    input_data:
               S3 folder path prefix where the origial log dataset resides
    output_data:
               S3 folder path where the dataset will be exported from Spark after data processing 
    Returns
    ---------------------
    This function will create two dimension tables and one fact table in S3, include "users" table, "time" table and somgplays table 
    
    """
    # get filepath to log data file
    log_data = input_data + "log_data/*/*/*"

    # read log data file
    df_log_data = spark.read.json(log_data)

    # extract columns for users table    
    users_table = df_log_data.select("userId", "firstName", "lastName", "gender", "level").dropDuplicates(subset=['userId'])
    
    # write users table to parquet files
    users_table.write.mode("overwrite").parquet(output_data + "users.parquet")

    # create timestamp column from original timestamp column
    convert_time = udf(lambda x: datetime.fromtimestamp(x/1000))
    df_time = df_log_data.select("ts").dropDuplicates()
    df_time = df_time.withColumn("timestamp", convert_time(df_time.ts))
    
    # create datetime column from original timestamp column
    convert_str = udf(lambda x: str(x))
    get_hour = udf(lambda x: x.hour)
    get_day = udf(lambda x: x.day)
    get_week = udf(lambda x: x.strftime('%W'))
    get_month = udf(lambda x: x.month)
    get_year = udf(lambda x: x.year)
    get_weekday = udf(lambda x: x.strftime('%A')) 
    
    
    # extract columns to create time table
    df_time = df_time.withColumn("start_time", convert_str(df_time["timestamp"]))
    df_time = df_time.withColumn("hour", get_hour(df_time["timestamp"]))
    df_time = df_time.withColumn("day", get_day(df_time["timestamp"]))
    df_time = df_time.withColumn("week", get_week(df_time["timestamp"]))
    df_time = df_time.withColumn("month", get_month(df_time["timestamp"]))
    df_time = df_time.withColumn("year", get_year(df_time["timestamp"]))
    df_time = df_time.withColumn("weekday", get_weekday(df_time["timestamp"]))
    df_time = df_time.select("start_time", "hour", "day", "week", "month", "year", "weekday").dropDuplicates(subset=['start_time'])
    
    # drop rows containing missing value
    df_time.dropna()
    
    # write time table to parquet files partitioned by year and month
    df_time.write.mode("overwrite").partitionBy('year', 'month').parquet(output_data + "time.parquet")


    # read in song data to use for songplays table
    song_data = input_data + "song_data/*/*/*/"
    df_song_data = spark.read.json(song_data)
    
    # add colume "start_time" and "songplay_id" into log_data dataframe
    convert_time_str = udf(lambda x: str(datetime.fromtimestamp(x/1000)))
    df_log_data = df_log_data.withColumn("start_time", convert_time_str(df_log_data.ts))
    df_log_data = df_log_data.withColumn('songplay_id', F.monotonically_increasing_id())
    df_log_data = df_log_data.filter(df_log_data.page == "NextSong")
    

    # joined song data, log data, time table to create songplays table 
    df_songplays_temp = df_log_data.join(df_song_data, (df_log_data.song == df_song_data.title) & (df_log_data.artist == df_song_data.artist_name), "inner").select(df_log_data.songplay_id, df_log_data.start_time,df_log_data.userId,df_log_data.level,df_song_data.song_id,df_song_data.artist_id,df_log_data.sessionId,df_log_data.location,df_log_data.userAgent)
    
    df_songplays_temp2 = df_songplays_temp.join(df_time, (df_songplays_temp.start_time == df_time.start_time),"inner").select(df_songplays_temp.songplay_id,df_songplays_temp.start_time,df_songplays_temp.userId,df_songplays_temp.level, df_songplays_temp.song_id, df_songplays_temp.artist_id,df_songplays_temp.sessionId, df_songplays_temp.location, df_songplays_temp.userAgent, df_time.year, df_time.month)
   
    df_songplays_final = df_songplays_temp2.dropDuplicates(subset=['songplay_id'])
    
    # write songplays table to parquet files partitioned by year and month
    df_songplays_final.write.mode("overwrite").partitionBy('year', 'month').parquet(output_data + "songplays.parquet")



def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = config['S3']['Data_path']
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
