# Twitter Scraping (ETL) of 2022 Elections, Police Brutality, and Hate Speech Data in Kenya

![Twitter ETL Process Workflow](https://github.com/IsaacMwendwa/Twitter-ETL-of-Elections-PoliceBrutality-HateSpeech-Data/blob/main/Images/scraping_work_flow.jpg "Twitter ETL Process Workflow")


## Table of Contents
* [Introduction](#Introduction)
* [Build_Tools](#Build_Tools)
* [Pre-requisites](#Pre-requisites)
* [ETL Process](#ETL-Process)
* [Database Normalization](#Database-Normalization)
* [Contributions](#Contributions)
* [Bug / Feature Request](#Bug--Feature-Request)
* [Authors](#Authors)


## Introduction
* This project is aimed at providing actionable insights to support United Nations' (UN) Sustainable Development Goal (SDG) Number 16.
* The SDGs are goals that are universally set by the UN Member States, to end poverty, protect the planet, and ensure peace and prosperity to all people.
* According to the UN, SDG Number 16 is directed to "Promote peaceful and inclusive societies for sustainable development, provide access to justice for all and build effective, accountable and inclusive institutions at all levels."
* This project is directed at supporting the achievement of SDG 16, by providing data to generate actionable insights to stakeholders regarding the 2022 Presidential Elections, Police Brutality, and Propagation of Hate Speech, in one of Kenya's most widely used social media platform -- Twitter.


## Build Tools
* Python 3
* Tweepy -- Python library for Twitter API
* PostgreSQL
* JSON
* SQLAlchemy
* Pandas


## Pre-requisites
You need to have the following to run 
* PostgreSQL RDBMS installed
* Twitter API Keys -- access tokens, consumer keys
* Python 3 environment with Tweepy, Pandas, Numpy, SQLALchemy, Json, and Glob libraries installed

## ETL Process
* ETL is short form for Extract, Transform, Load -- a three phase integration process for getting data to a data warehouse

### Phase 1 of ETL: Extract
To extract the data(tweets) from Twitter, we create a Streaming Pipeline using the below steps:
1. Import the Tweepy package, and then authenticate from the Twitter API using access tokens and consumer keys
2. Specify keywords (filtering) that would be used to stream the tweets. This will revolve around the 2022 Presidential Elections, Police Brutality, and Propagation of Hate Speech
3. Instantiate `SListener` and `Stream` objects, and loop the streaming filter to run continuously in a streaming fashion
4. Include an `except` clause to handle network disconnections, by reconnecting streaming automatically
5. The streamed tweets (in `SListener` object) will be dumped in JSON files (staging area), using Python's file writing operation. Note that the filename is derived from the time the file was being written, and each file is capped at 500 tweets

![Extracted Tweets Dumped in JSON files](https://github.com/IsaacMwendwa/Twitter-ETL-of-Elections-PoliceBrutality-HateSpeech-Data/blob/main/Images/Extracted-Tweets-Dumped-in-JSON-file.PNG "Extracted Tweets Dumped in JSON files")


### Phase 2 of ETL: Transform
Note that the JSON files created are a list of key: value pairs, containing information about the tweet (e.g time of creation, tweet text, tweet sender). \
To transform the data, we create a Data Transform Pipeline using the below steps:
1. Create a function `json2df`, which takes a JSON filename as a parameter; to load the JSON files into a list of Python dictionaries
2. Utilize Pandas to convert the dictionaries to Pandas DataFrames
3. Add the converted dataframes to a list named `to_sql`, where each dataframe corresponds to a table in the database
4. The function `json2df` then returns the `to_sql` list, which will be used for the last step of the ETL process

### Phase 3 of ETL: Load
To load the data to the PostgreSQL data warehouse, we create a Data Load Pipeline as below:
1. Create a new database in the PostgreSQL RDBMS. \
   This can be achieved graphically using [pgAdmin 4](https://www.pgadmin.org/) (management system for Postgres), or programmatically as below:
\
\
![Creating a New Database](https://github.com/IsaacMwendwa/Twitter-ETL-of-Elections-PoliceBrutality-HateSpeech-Data/blob/main/Images/Creating-a-New-Database.PNG "Creating a New Database")
2. To create tables for data storage in the database, we will employ SQLAlchemy \
   SQLAlchemy allows creation of tables from Pandas DataFrames automatically, without the need for explicitly specifying columns and column types for each table. \
   This comes in handy, as we have lots of tables and columns to load into our database
4. Proceed to load data into the database tables from the Pandas DataFrames, by utilizing the `Pandas.DataFrame.to_sql()` function
\
\
![Load Pandas Dataframes to Database](https://github.com/IsaacMwendwa/Twitter-ETL-of-Elections-PoliceBrutality-HateSpeech-Data/blob/main/Images/Load-Pandas-Dataframes-to-Database.PNG "Load Pandas Dataframes to Database")

5. The schema of the created database is as below:
\
\
![Tables Created in Database](https://github.com/IsaacMwendwa/Twitter-ETL-of-Elections-PoliceBrutality-HateSpeech-Data/blob/main/Images/tables_in_database.jpg "Tables Created in Database")


## Database Normalization
* Database normalization is a database design principle for organizing data in an organized and consistent manner
* It is directed at avoiding redundancy and maintaining integrity of the database; by avoiding complexities and eliminating duplicates
* In normalization, data is divided into several tables linked together with relationships; achieved using primary, foreign and composite keys
* We note that the tables created using SQLAlchemy do not have primary/foreign keys specified, nor do they have any explicit relationships defined -- implying a lot of redundancy in the tables
* To correct this, we do the following to normalize our database:
   *  Add constraints to the tables by creating primary and foreign keys in the tables
   *  Delete duplicate records in the tables
   *  Delete unnecessary columns from the tables
* The normalized database will now look like as below:

![Normalized Database Schema](https://github.com/IsaacMwendwa/Twitter-ETL-of-Elections-PoliceBrutality-HateSpeech-Data/blob/main/Images/Full-Schema-of-Normalized-Database.jpg "Normalized Database Schema")

* This can be summarized to the below forms:

   * Normalized Relational Database Schema Showing Only Primary Keys
   * ![Database Schema Showing Only Primary Keys](https://github.com/IsaacMwendwa/Twitter-ETL-of-Elections-PoliceBrutality-HateSpeech-Data/blob/main/Images/Relational-Database-Schema-Showing-Only-Primary-Keys.jpg "Database Schema Showing Only Primary Keys")

   * Normalized Relational Database Schema Showing Only Relationships
   * ![Database Schema Showing Only Relationships](https://github.com/IsaacMwendwa/Twitter-ETL-of-Elections-PoliceBrutality-HateSpeech-Data/blob/main/Images/Relational-Database-Schema-Showing-Only-Relationships.jpg "Database Schema Showing Only Relationships")


## Contributions
Contributions are welcome using pull requests. To contribute, follow these steps:
1. Fork this repository.
2. Create a branch: `git checkout -b <branch_name>`
3. Make your changes to relevant file(s)
4. Check status of your commits: `git status`
6. Add and commit file(s) to the repo using:
    `git add <file(s)>`
    `git commit -m "<message>"`
8. Push repo to Github: `git push origin <branch_name`
9. Create the pull request. See the GitHub documentation on [creating a pull request](https://help.github.com/en/github/collaborating-with-issues-and-pull-requests/creating-a-pull-request).

## Bug / Feature Request
If you find a bug (the website couldn't handle the query and/or gave undesired results), kindly open an issue [here](https://github.com/IsaacMwendwa/Twitter-ETL-of-Elections-PoliceBrutality-HateSpeech-Data/issues/new) by including your search query and the expected result.

If you'd like to request a new function, feel free to do so by opening an issue [here](https://github.com/IsaacMwendwa/Twitter-ETL-of-Elections-PoliceBrutality-HateSpeech-Data/issues/new). Please include sample queries and their corresponding results.


## Authors

* **[Isaac Mwendwa](https://github.com/IsaacMwendwa)**
    
[![github follow](https://img.shields.io/github/followers/IsaacMwendwa?label=Follow_on_GitHub)](https://github.com/IsaacMwendwa)


See also the list of [Contributors](https://github.com/IsaacMwendwa/Twitter-ETL-of-Elections-PoliceBrutality-HateSpeech-Data/contributors) who participated in this project.

