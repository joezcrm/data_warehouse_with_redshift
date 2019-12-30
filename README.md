The project creates a star schema and insert data from files. One fact table and four dimension are created as the following:
## Fact Table:
### songplays
All columns except **songplay_id** are nullable.  
>CREATE TABLE IF NOT EXISTS **songplays**    (    
>**songplay_id** BIGINT IDENTITY(0,1) NOT NULL PRIMARY KEY,   
>**start_time** TIMESTAMP NOT NULL sortkey,   
>**user_id** INTEGER NOT NULL distkey,   
>**level** VARCHAR,   
>**song_id** VARCHAR NOT NULL,   
>**artist_id** VARCHAR NOT NULL,   
>**session_id** INTEGER NOT NULL,   
>**location** VARCHAR,   
>**user_agent** VARCHAR );   

## Dimension Tables:
### users
All columns except **user_id** are nullable and only distinct records are accepted.
>CREATE TABLE IF NOT EXISTS **users** (   
>**user_id**              INTEGER NOT NULL PRIMARY KEY sortkey,   
>**first_name**           VARCHAR,  
>**last_name**            VARCHAR,  
>**gender**               VARCHAR,  
>**level**                VARCHAR  
>) diststyle **all**;

### songs  
All columns except **song_id** are nullable and only distinct records are acccepted.
>CREATE TABLE IF NOT EXISTS **songs** (   
>**song_id**              VARCHAR NOT NULL PRIMARY KEY sortkey,   
>**title**                VARCHAR,   
>**artist_id**            VARCHAR NOT NULL distkey,   
>**year**                 INTEGER,   
>**duration**             DECIMAL(9,5) );

### artists
All columns except **artist_id** are nullable and only distinct records are accepted.
>CREATE TABLE IF NOT EXISTS **artists** (   
>**artist_id**            VARCHAR NOT NULL PRIMARY KEY sortkey,   
>**name**                 VARCHAR,   
>**location**             VARCHAR,   
>**latitude**             DECIMAL(9,5),   
>**longitude**            DECIMAL(9,5) )   
>diststyle **all**;

### time
All records of time are truncated to include only the date and hour parts.  Only distinct records of time will be inserted.  For weekdays, Sunday is 0, Monday is 1, Tuesday is 2, and so on.
>CREATE TABLE IF NOT EXISTS **time**  (  
>**start_time**           TIMESTAMP NOT NULL PRIMARY KEY sortkey,  
>**hour**                 INTEGER,  
>**day**                  INTEGER,  
>**week**                 INTEGER,  
>**month**                INTEGER,  
>**year**                 INTEGER,  
>**weekday**              INTEGER )  
>diststyle **all**;

Two staging tables are also created to copy data from files. All columns of the staging tables are nullable so that data can be loaded successfully:  
## Staging Tables:
### staging_events_table
>CREATE TABLE IF NOT EXISTS **staging_events_table** (  
>**artist**               VARCHAR,  
>**auth**                 VARCHAR,  
>**first_name**           VARCHAR,  
>**gender**               VARCHAR,  
>**item_in_session**      INTEGER,  
>**last_name**            VARCHAR,  
>**length**               DECIMAL(9,5),  
>**level**                VARCHAR,  
>**location**             VARCHAR,  
>**method**               VARCHAR,  
>**page**                 VARCHAR,  
>**registration**         BIGINT,  
>**session_id**           INTEGER,  
>**song**                 VARCHAR,  
>**status**               INTEGER,  
>**ts**                   BIGINT,  
>**user_agent**           VARCHAR,  
>**user_id**              INTEGER distkey );  



### staging_songs_table
>CREATE TABLE IF NOT EXISTS **staging_songs_table** (  
>**num_songs**            BIGINT,  
>**artist_id**            VARCHAR distkey,  
>**artist_latitude**      DECIMAL(9,5),  
>**artist_longitude**     DECIMAL(9,5),  
>**artist_location**      VARCHAR,  
>**artist_name**          VARCHAR,  
>**song_id**              VARCHAR,  
>**title**                VARCHAR,  
>**duration**             DECIMAL(9,5),  
>**year**                 INTEGER );  


After records are copied into the staging tables, the corresponding columns are selected and inserted into fact and dimension tables.  The staging tables will be kept in the cloud until they are not needed.

## Examples of Database Usage
In order to find the pattern of users' activities, a data consumer can query as follow:
>SELECT **songplays.start_time**, **COUNT(songplays.start_time)** AS **count_tm**   
>From **songplays** JOIN **time**  
>ON **songplays.start_time** = **time.start_time**  
>WHERE **time.year** = **2018**  
>AND **time.month** = **10**  
>GROUP BY **songplays.start_time**
>ORDER BY **songplays.start_time**;  

To find the most popular artists and to retrieve their informations, a data consumer can do the following:
>SELECT **artists.artist_id**, **artists.name**, **count_table.count_artist** FROM (  
>SELECT **artist_id**, **COUNT(artist_id)** AS **count_artist**  
>FROM **songplays**  
>GROUP BY **artist_id**    
>) AS **count_table**  
>JOIN **artists**  
>ON **count_table.artist_id** = **artists.artist_id**  
>ORDER BY **count_table.count_artist** DESC  
>LIMIT **10**;