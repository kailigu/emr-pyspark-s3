import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from pyspark.sql.types import *


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """
        Loads song_data from S3 and select the data to get 2 tables - songs and artists 
        Loads 2 tables back to 33
        
        Parameters:
            spark       : Spark Session
            input_data  : path of song_data 
            output_data : path of target S3 bucket
    """
    
    
    # get filepath to song data file
    song_data = input_data + "song_data/*/*/*/*"
    
    songSchema = StructType([
    StructField("artist_id",StringType()),
    StructField("artist_latitude",DoubleType()),
    StructField("artist_location",StringType()),
    StructField("artist_longitude",DoubleType()),
    StructField("artist_name",StringType()),
    StructField("duration",DoubleType()),
    StructField("num_songs",DoubleType()),
    StructField("song_id",StringType()),
    StructField("title",StringType()),
    StructField("year",IntegerType()),
    ])
    
    # read song data file
    df_song = spark.read.json(song_data, schema=songSchema)

    # extract columns to create songs table
    songs_cols = ["song_id","title", "artist_id","year", "duration"]
    songs_table = df_song.select(songs_cols).dropDuplicates()
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.parquet(output_data + "songs/", mode="overwrite", partitionBy=["year","artist_id"])

    # extract columns to create artists table
    artists_cols = ["artist_id", "artist_name as name", "artist_location as location", "artist_latitude as latitude", "artist_longitude as longitude"]
    artists_table = df_song.selectExpr(artists_cols).dropDuplicates()
    
    # write artists table to parquet files
    artists_table.write.parquet(output_data + "artists/", mode="overwrite")


def process_log_data(spark, input_data, output_data):    
    """
        Loads log_data from S3 and transform the data to get 2 tables - users and time
        Loads 2 transformed table - songs and artisits from s3
        Joins 2 tables - songs, artists - with original log_data to get the fact table
        
        Parameters:
            spark       : Spark Session
            input_data  : path of log_data 
            output_data : path of target S3 bucket
    """
    
    # get filepath to log data file
    log_data = input_data + "log_data/*/*/*"

    # read log data file
    df_log = spark.read.json(log_data)
    
    # filter by actions for song plays
    df_log = df_log.filter(df_log.page == 'NextSong')

    # extract columns for users table    
    users_cols = ["userId as user_id", "firstName as first_name", "lastName as last_name", "gender", "level"]
    users_table = df_log.selectExpr(users_cols).dropDuplicates()
    
    # write users table to parquet files
    users_table.write.parquet(output_data + 'users/')

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x : datetime.utcfromtimestamp(int(x)/1000), TimestampType())
    df_log = df_log.withColumn("start_time", get_timestamp("ts"))
    
    # extract columns to create time table
    df_log = df_log.withColumn("hour", hour("start_time"))\
                .withColumn("day", dayofmonth("start_time"))\
                .withColumn("week", weekofyear("start_time"))\
                .withColumn("month", month("start_time"))\
                .withColumn("year", year("start_time"))\
                .withColumn("weekday", dayofweek("start_time"))\
    
    # write time table to parquet files partitioned by year and month
    time_table = df_log.select("start_time","hour","day","week","month","year","weekday").drop_duplicates()
    time_table.write.parquet(output_data + 'time/', mode='overwrite', partitionBy=["year","month"])

    # read in song data to use for songplays table
    df_song = spark.read.parquet(output_data + 'songs/*/*/*')
    df_artists = spark.read.parquet(output_data + 'artists/*')
    df_artists = df_artists.select("artist_id", "name")

    # extract columns from joined song and log datasets to create songplays table 
    df_songplays = df_log.join(df_song, df_log.song == df_song.title, how='left')
    df_songplays = df_songplays.join(df_artists, df_songplays.artist == df_artists.name, how='left')
    
    songplays_cols = ['start_time','userId as user_id','level','song_id','artist_id','sessionId as session_id',\
                  'location', 'userAgent as user_agent','year','month']
    df_songplays = df_songplays.selectExpr(songplays_cols).drop_duplicates()
    df_songplays = df_songplays.withColumn("songplay_id", monotonically_increasing_id())

    # write songplays table to parquet files partitioned by year and month
    df_songplays.write.parquet(output_data + "songplays/", mode="overwrite", partitionBy=["year","month"])


def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = ""
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
