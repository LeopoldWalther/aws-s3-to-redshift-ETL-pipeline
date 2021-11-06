# aws-s3-to-redshift-ETL-pipeline

## Introduction
A music streaming startup, Sparkify, has grown their user base and song database and want to move their processes and data onto the cloud. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

Goal of this project is to build an ETL pipeline that extracts their data from S3, stages them in Redshift, and transforms data into a set of dimensional tables for their analytics team to continue finding insights in what songs their users are listening to.

## Project Description
This project ist about applying the learned content about data warehouses and AWS to build an ETL pipeline for a database hosted on Redshift. Part of the project is to load data from S3 to staging tables on Redshift and execute SQL statements that create the analytics tables from these staging tables.

## Project Structure

## Data Model

### Fact Table

#### Table "songplays"

| COLUMN      | TYPE      | CONSTRAINT  |
|---          |---	      |---	        |
| songplay_id	| SERIAL  	| PRIMARY KEY	|
| start_time	| TIMESTAMP	| NOT NULL   	|
| user_id	    | int	      | NOT NULL	  |
| level	      | varchar   |   	        |
| song_id	    | varchar	  |   	        |
| artist_id	  | varchar	  |   	        |
| session_id	| int	      |   	        |
| location	  | text	    |   	        |
| user_agent	| text	    |   	        |


 ### Dimensions Tables
 Following the star shape there is a Dimensions table for each dimension in the Fact Table.


 #### Table "users"

| COLUMN  	  | TYPE  	   | CONSTRAINT  	|
|---	        |---	       |---	          |
| user_id	    | int  	     | PRIMARY KEY	|
| first_name	| varchar	   |  	          |
| last_name	  | varchar	   |  	          |
| gender	    | varchar(1) |   	          |
| level	      | varchar	   |   	          |


#### Table "songs"

| COLUMN  	  | TYPE  	   | CONSTRAINT   	|
|---	        | ---	       |---	            |
| song_id	    | varchar  	 | PRIMARY KEY	  |
| title	      | text	     |  	            |
| artist_id	  | varchar	   |   	            |
| year	      | int        |   	            |
| duration	  | numeric	   |   	            |


#### Table "artists"

| COLUMN  	  | TYPE  	    | CONSTRAINT   	|
|---	        | ---	        |---	          |
| artist_id	  | varchar  	  | PRIMARY KEY	  |
| name	      | varchar	    |   	          |
| location	  | text	      |   	          |
| latitude	  | decimal	    |   	          |
| longitude	  | decimal     |   	          |


#### Table "time"

| COLUMN  	  | TYPE  	    | CONSTRAINT   	|
|---	        |---	        |---	          |
| start_time	| TIMESTAMP  	| PRIMARY KEY	  |
| hour	      | int	        |   	          |
| day	        | int	        |   	          |
| week	      | int	        |   	          |
| month	      | int	        |   	          |
| year	      | int	        |   	          |
| weekday	    | varchar	    |   	          |

## Local Execution

## Licensing, Authors, Acknowledgements