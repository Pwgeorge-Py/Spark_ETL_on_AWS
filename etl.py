import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import udf, col, row_number, count
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format, from_unixtime, dayofweek
from pyspark.sql.types import StructType as R, StructField as Fld, DoubleType as Dbl, StringType as Str, IntegerType as Int, DateType as Date, LongType as Long

config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']

def create_spark_session():
    """
    Function creates and returns SparkSession object
    """
    
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """
    This function loads the songs JSON dataset from S3, 
    then uses the data to create the songs and artists tables
    
    Input:
    spark = SparkSession object
    input_data = Start of path variable for input files
    output_data = Start of path variable for output files
    
    Output: None
    """
    
    
    # get filepath to song data file
    song_data = os.path.join(input_data, 'song_data/*/*/*/*.json')
    
    # Define schema
    SongSchema = R([
        Fld("num_songs", Int()),
        Fld("artist_id", Str()),
        Fld("artist_latitude", Dbl()),
        Fld("artist_longitude", Dbl()),
        Fld("artist_location", Str()),
        Fld("artist_name", Str()),
        Fld("song_id", Str()),
        Fld("title", Str()),
        Fld("duration", Dbl()),
        Fld("year", Int())
    ])

    # read song data file
    df = spark.read.json(song_data, schema=SongSchema)

    # extract columns to create songs table
    songs_table = df.select("song_id","title","artist_id","year","duration").dropduplicates()
    
    # write songs table to parquet files partitioned by year and artist
    output_path = os.path.join(output_data, 'songs_table.parquet')
    songs_table.write.partitionBy("year","artist_id").parquet(output_path, mode="overwrite")

    # extract columns to create artists table
    artists_table = df.select("artist_id","artist_name","artist_location","artist_latitude","artist_longitude").dropduplicates()
    
    # write artists table to parquet files
    output_path = os.path.join(output_data, 'artists_table.parquet')
    artists_table.write.parquet(output_path, mode="overwrite")
    
    #export whole songs data file to parquet
    output_path = os.path.join(output_data, 'songs_data_table.parquet')
    df.write.parquet(output_path, mode="overwrite")


def process_log_data(spark, input_data, output_data):
    """
    This function loads the log JSON dataset from S3, 
    then uses the data to create the users, time and songplays tables
    
    Input:
    spark = SparkSession object
    input_data = Start of path variable for input files
    output_data = Start of path variable for output files
    
    Output: None
    """
    
    # get filepath to log data file
    log_data = os.path.join(input_data, 'log_data/*.json')
    song_data_parq_path = os.path.join(output_data, 'songs_data_table.parquet')
    
    # read log data file
    df = spark.read.json(log_data)
    
    # filter by actions for song plays
    df = df.where(df.page == 'NextSong')
    
    # extract columns for users table    
    users_table = df.select("userId","firstName","lastName","gender","level").dropduplicates()
    
    # write users table to parquet files
    output_path = os.path.join(output_data, 'users_table.parquet')
    users_table.write.parquet(output_path, mode="overwrite")

    # create timestamp column from original timestamp column
    df = df.withColumn('Timestamp', from_unixtime(df.ts/1000, 'yyyy-MM-dd HH:mm:ss'))
    
    # extract columns to create time table
    time_table = df.select(col('Timestamp').alias('start_time'), hour(col('Timestamp')).alias("hour"), dayofmonth(col('Timestamp')).alias("day"), \
                           weekofyear(col('Timestamp')).alias("week"), month(col('Timestamp')).alias("month"), \
                           year(col('Timestamp')).alias("year"), dayofweek(col('Timestamp')).alias("weekday")).dropduplicates()

    # write time table to parquet files partitioned by year and month
    output_path = os.path.join(output_data, 'time_table.parquet')
    time_table.write.partitionBy("year","month").parquet(output_path, mode="overwrite")

    #Create row number column to use as songplay_id
    windowSpec = Window.orderBy('Timestamp')
    df = df.withColumn('songplay_id', row_number().over(windowSpec))

    # read in song data to use for songplays table
    song_data_df = spark.read.parquet(song_data_parq_path).select("song_id","title","artist_id","duration","artist_name").dropduplicates()
    df.createOrReplaceTempView("logs")
    song_data_df.createOrReplaceTempView("songs")
    songplays_df = spark.sql("""
                            SELECT DISTINCT
                                logs.songplay_id ,
                                logs.Timestamp as start_time,
                                logs.userId as user_id,
                                logs.level,
                                songs.song_id,
                                songs.artist_id,
                                logs.sessionId as session_id,
                                logs.location,
                                logs.userAgent as user_agent,
                                EXTRACT(year FROM Timestamp) AS year,
                                EXTRACT(month FROM Timestamp) AS month
                            FROM
                                logs
                            JOIN
                                songs 
                                    ON songs.artist_name = logs.artist
                                    AND songs.title = logs.song
                                    AND songs.duration = logs.length
                        """)

    # write songplays table to parquet files partitioned by year and month
    output_path = os.path.join(output_data, 'songplays_table.parquet')
    songplays_df.write.partitionBy("year","month").parquet(output_path, mode="overwrite")

def main():
    """
    Main function to execute functions in correct sequence and create variables
    
    Input: None
    Output: None
    """
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://sparkify-output594/tables/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
