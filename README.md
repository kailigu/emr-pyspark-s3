
# Project Introduction
A music streaming startup, Sparkify, has grown their user base and song database even more and want to move their data warehouse to a data lake. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.


## How to run this project

1. Fill in the configuration file `dl.cfg` to connnect to AWS EMR:

```
KEY=YOUR_AWS_ACCESS_KEY
SECRET=YOUR_AWS_SECRET_KEY
```

2. Create an S3 bucket to store the output of this project

3. Run the command:
```
python3 etl.py
```


## File structure

- dl.cfg: Configuration file that stores AWS credentials
- etl.py: Main file that impleted the ETL pipeline
- README.md: Description of this project 


## ETL Pipeline

1. Load AWS credentials and create a spark session connecting to AWS EMR
    
2. Load data from S3
    
    -   Song data:  `s3://udacity-dend/song_data`
    -   Log data:  `s3://udacity-dend/log_data`
    
    The script reads song_data and load_data from S3.
    
3. Tranform and select from song_data to get 2 dimensional tables:

	 **songs**  
    Fields - _song_id, title, artist_id, year, duration_
    
	**artists** 
    Fields -   _artist_id, name, location, lattitude, longitude_

4. Transform and select from log_data to get 2 dimensional tables：

	**users**  
		Fields -   _user_id, first_name, last_name, gender, level_
    
    **time**  
    Fields -   _start_time, hour, day, week, month, year, weekday_
    
5. Join log_data with dimensional tables got from Stage 3 - songs, artists - to get the fact table:

	**songplays**  
    -   _songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent_        

6. Load above tables as partitioned parquet files to S3
    
    
