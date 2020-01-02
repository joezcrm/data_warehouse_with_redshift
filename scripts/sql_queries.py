import configparser


# CONFIG, get the parameters
config = configparser.ConfigParser()
config.read('dwh.cfg')
access_key = config.get("AWS", "ACCESS_KEY")
secret_key = config.get("AWS", "SECRET_KEY")
log_data_source = config.get("S3", "LOG_DATA")
log_jsonpath = config.get("S3", "LOG_JSONPATH")
song_data_source = config.get("S3", "SONG_DATA")
song_jsonpath = config.get("S3", "SONG_JSONPATH")

# Queries to drop tables

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events_table;"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs_table;"
songplay_table_drop = "DROP TABLE IF EXISTS songplays;"
user_table_drop = "DROP TABLE IF EXISTS users;"
song_table_drop = "DROP TABLE IF EXISTS songs;"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time;"

# Create staging tables to be used to copy data to

# Columns of staging_events_table are identical as those from log data files
# Data is distributed across the cloud using the column "user_id"
staging_events_table_create= ("""
CREATE TABLE IF NOT EXISTS staging_events_table (
artist               VARCHAR,
auth                 VARCHAR,
first_name           VARCHAR,
gender               VARCHAR,
item_in_session      INTEGER,
last_name            VARCHAR,
length               DECIMAL(9,5),
level                VARCHAR,
location             VARCHAR,
method               VARCHAR,
page                 VARCHAR,
registration         BIGINT,
session_id           INTEGER,
song                 VARCHAR,
status               INTEGER,
ts                   BIGINT sortkey,
user_agent           VARCHAR,
user_id              INTEGER distkey
);
""")

# Columns of staging_songs_table are identical as those from song data files
# Data is distributed using the column "artist_id"
staging_songs_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_songs_table(
num_songs            BIGINT,
artist_id            VARCHAR distkey,
artist_latitude      DECIMAL(9,5),
artist_longitude     DECIMAL(9,5),
artist_location      VARCHAR,
artist_name          VARCHAR,
song_id              VARCHAR sortkey,
title                VARCHAR,
duration             DECIMAL(9,5),
year                 INTEGER 
);
""")

# Create tables for the final results

# The fact table of the schema
# The data is distributed across the cloud using "user_id"
# The rows are sorted using start_time
songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplays (
songplay_id          BIGINT IDENTITY(0,1) NOT NULL PRIMARY KEY,
start_time           TIMESTAMP NOT NULL sortkey,
user_id              INTEGER NOT NULL distkey,
level                VARCHAR,
song_id              VARCHAR NOT NULL,
artist_id            VARCHAR NOT NULL,
session_id           INTEGER NOT NULL,
location             VARCHAR,
user_agent           VARCHAR
);
""")

# Dimension table
# The table uses "all" distribution style
user_table_create = ("""
CREATE TABLE IF NOT EXISTS users (
user_id              INTEGER NOT NULL PRIMARY KEY sortkey,
first_name           VARCHAR,
last_name            VARCHAR,
gender               VARCHAR,
level                VARCHAR
) diststyle all;
""")

# Dimension table
# The rows are distributed across the cloud using the column "artist_id"
song_table_create = ("""
CREATE TABLE IF NOT EXISTS songs (
song_id              VARCHAR NOT NULL PRIMARY KEY sortkey,
title                VARCHAR,
artist_id            VARCHAR NOT NULL distkey,
year                 INTEGER,
duration             DECIMAL(9,5)
);
""")

# Dimension table
# The table uses "all" distribution style
artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artists (
artist_id            VARCHAR NOT NULL PRIMARY KEY sortkey,
name                 VARCHAR,
location             VARCHAR,
latitude             DECIMAL(9,5),
longitude            DECIMAL(9,5))
diststyle all;
""")

# Dimension table
# The table uses "all" distribution style
time_table_create = ("""
CREATE TABLE IF NOT EXISTS time(
start_time           TIMESTAMP NOT NULL PRIMARY KEY sortkey,
hour                 INTEGER,
day                  INTEGER,
week                 INTEGER,
month                INTEGER,
year                 INTEGER,
weekday              INTEGER)
diststyle all;
""")

# Copy data to the staging tables from existing files

# Copy from log data files to staging_events_table
staging_events_copy = ("""
COPY staging_events_table
FROM {}
access_key_id {}
secret_access_key {}
json {}
region 'us-west-2';
""").format(log_data_source, access_key, secret_key, log_jsonpath)

# Copy from song data files to staging_songs_table
staging_songs_copy = ("""
COPY staging_songs_table 
FROM {}
access_key_id {}
secret_access_key {}
json {}
region 'us-west-2';
""").format(song_data_source, access_key, secret_key, song_jsonpath)


# Select data from staging tables
# Insert data to the final tables

# Join staging_songs_table and staging_events_table to get the "song_id" and "artist_id"
# Insert the rest of data from staging_events_table into songplays table
songplay_table_insert = ("""
INSERT INTO songplays (start_time, user_id, level, 
                            song_id, artist_id, session_id, location, user_agent)
    SELECT DISTINCT dateadd(hr, EXTRACT(hr FROM se.tm),
                        dateadd(day, EXTRACT(day FROM se.tm),
                            dateadd(month, EXTRACT(month FROM se.tm), 
                                dateadd(year, EXTRACT(year FROM se.tm) - 
                                    EXTRACT(year FROM 'epoch'::timestamp), 'epoch')))),
        se.user_id, se.level, ss.song_id, ss.artist_id, 
            se.session_id, se.location, se.user_agent
    FROM (
        SELECT artist_id, artist_name, song_id, title, duration
        FROM staging_songs_table
        WHERE artist_id IS NOT NULL
        AND song_id IS NOT NULL
        ) AS ss
    JOIN (
        SELECT dateadd(ms,ts,'epoch') AS tm, user_id, level, song, artist, 
            length, session_id, location, user_agent
        FROM staging_events_table 
        WHERE page = 'NextSong'
        AND ts IS NOT NULL
        AND user_id IS NOT NULL
        ) AS se
    ON ss.artist_name = se.artist
    AND ss.title = se.song
    AND ss.duration = se.length
;
""")

# Select data from staging_events_table
# Insert into users table
# Limit "user_id" to be NOT NULL
# Assuming the record with larger "ts" is newer
# Need to communicate with the team about the actual details in real life
user_table_insert = ("""
INSERT INTO users (user_id, first_name, last_name, gender, level)
    SELECT DISTINCT id_only_table.user_id, first_name, last_name, gender, level 
    FROM 
        (
        SELECT user_id, MAX(ts) AS mts 
        FROM staging_events_table
        WHERE page = 'NextSong'
        AND user_id IS NOT NULL
        AND ts IS NOT NULL
        GROUP BY user_id
        ) AS id_only_table
    JOIN
        (
        SELECT user_id, first_name, last_name, gender, level, ts
        FROM staging_events_table
        WHERE page = 'NextSong' 
        AND user_id IS NOT NULL
        AND ts IS NOT NULL
        ) AS all_columns_table
    ON id_only_table.user_id = all_columns_table.user_id
    AND id_only_table.mts = all_columns_table.ts
;
""") 

# Select data from staging_songs_table
# Insert into songs table
# Limit "song_id" and "artist_id" to be NOT NULL
# Assuming "song_id" and "num_songs" uniquely identify each record
# And the record with larger "num_songs" is newer
# Need to communicate with the team in real life
song_table_insert = ("""
INSERT INTO songs (song_id, title, artist_id, year, duration)
    SELECT DISTINCT id_only_table.song_id, title, artist_id, year, duration
    FROM
        (
        SELECT song_id, MAX(num_songs) AS m_num
        FROM staging_songs_table
        WHERE song_id IS NOT NULL
        AND artist_id IS NOT NULL
        AND num_songs IS NOT NULL
        GROUP BY song_id
        ) AS id_only_table
    JOIN
        (
        SELECT song_id, title, artist_id, year, duration, num_songs
        FROM staging_songs_table
        WHERE song_id IS NOT NULL
        AND artist_id IS NOT NULL
        AND num_songs IS NOT NULL
        ) all_columns_table
    ON id_only_table.song_id = all_columns_table.song_id
    AND id_only_table.m_num = all_columns_table.num_songs
;
""")

# Select data from stagging_songs_table
# Insert into artists table
# Limit the "song_id" and "artist_id" to be NOT NULL
# Assuming "song_id" and "num_songs" uniquely identify each record
# And record with larger "num_songs" is newer
# Actual communication needed
artist_table_insert = ("""
INSERT INTO artists (artist_id, name, location, latitude, longitude)
    SELECT  DISTINCT artist_id, 
            artist_name,
            artist_location,
            artist_latitude,
            artist_longitude
    FROM
        (
        SELECT song_id, MAX(num_songs) AS m_num
        FROM staging_songs_table
        WHERE song_id IS NOT NULL
        AND artist_id IS NOT NULL
        AND num_songs IS NOT NULL
        GROUP BY song_id
        ) AS id_only_table
    JOIN
        (
        SELECT song_id, num_songs, artist_id, artist_name, artist_location, 
            artist_latitude, artist_longitude, year
        FROM staging_songs_table
        WHERE song_id IS NOT NULL
        AND artist_id IS NOT NULL
        AND num_songs IS NOT NULL
        ) AS all_columns_table
    ON id_only_table.song_id = all_columns_table.song_id
    AND id_only_table.m_num = all_columns_table.num_songs
;     
""")

# Select ts (BIGINT) from staging_events_table and limit the data to be NOT NULL
# Convert ts to TIMESTAMP and truncate the data to date and hour
# Extract hour, day, week, month, year, weekday part from the truncated timestamp
# Insert the result in time table
# For weekdays, Sunday is 0, Monday is 1, Tuesday is 2, and so on
time_table_insert = ("""
INSERT INTO time (start_time, hour, day, week, month, year, weekday)
SELECT  trunc_time, 
        EXTRACT(hour FROM trunc_time), 
        EXTRACT(day FROM trunc_time),
        EXTRACT(week FROM trunc_time),
        EXTRACT(month FROM trunc_time),
        EXTRACT(year FROM trunc_time),
        EXTRACT(weekday FROM trunc_time)
FROM
(
    SELECT DISTINCT dateadd(hr, EXTRACT(hr FROM tm),
                dateadd(day, EXTRACT(day FROM tm),
                    dateadd(month, EXTRACT(month FROM tm),
                        dateadd(year, EXTRACT(year FROM tm) - 
                          EXTRACT(year FROM 'epoch'::timestamp), 'epoch')))) 
        AS trunc_time
    FROM
    (
        SELECT dateadd(ms, ts, 'epoch') AS tm
        FROM staging_events_table 
        WHERE page = 'NextSong'
        AND ts IS NOT NULL
    )
);    
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, \
                        songplay_table_create, user_table_create, song_table_create, \
                        artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, \
                      songplay_table_drop, user_table_drop, song_table_drop, \
                      artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, \
                        song_table_insert, artist_table_insert, time_table_insert]
