<h2>Project goals</h2>

The purpose of this pipeline and database is to gain analysis insights on the consumer habits who use the music streaming app Sparkify.
Data is collected by the app in JSON format and is stored in an AWS S3 bucket.
The 2 source datasets are: 
Songs dataset which stores information about all the songs/artists on the platform.
Log dataset which stores information about activities users are performing on the platform such as listening to a song.

The end goal here is to get this data into a format that can allow a user to perform analytical queries to hopefully derive buisness insights.


<h2>Design Decisions</h2>

The ETL.py script uses spark to load our source data from an S3 bucket stored in JSON bucket, this is done with an AWS EMR cluster because of the datasets size.
Using spark we convert the data into a star schema optimized for queries on song play analysis, we save each table in parquet format and save it back in S3.
This data can then be loaded later on a spark EMR or onto AWS Athena and queried as needed
From that 2 source datasets I have created 5 destination tables:
Songplays
Users
Songs
Artists
Time
