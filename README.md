# Twitter Scraping (ETL) of 2022 Elections, Police Brutality, and Hate Speech Data in Kenya

![Twitter ETL Process Workflow](https://github.com/IsaacMwendwa/Twitter-ETL-of-Elections-PoliceBrutality-HateSpeech-Data/blob/main/Images/scraping_work_flow.jpg "Twitter ETL Process Workflow")


## Table of Contents
* [Introduction](#Introduction)
* [Build_Tools](#Build_Tools)
* [Pre-requisites](#Pre-requisites)
* [ETL Process](#ETL-Process)
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
To extract the data(tweets) from Twitter, we create a streaming pipeline using the below steps:
1. Import the Tweepy package, and then authenticate from the Twitter API using access tokens and consumer keys
2. Specify keywords (filtering) that would be used to stream the tweets. This will revolve around the 2022 Presidential Elections, Police Brutality, and Propagation of Hate Speech
3. Instantiate `SListener` and `Stream` objects, and loop the streaming filter to run continuously in a streaming fashion
4. Include an `except` clause to handle network disconnections, by reconnecting streaming automatically
5. The streamed tweets (in `SListener` object) will be dumped in JSON files (staging area), using Python's file writing operation. Note that the filename is derived from the time the file was being written, and each file is capped at 500 tweets

![Extracted Tweets Dumped in JSON files](https://github.com/IsaacMwendwa/Twitter-ETL-of-Elections-PoliceBrutality-HateSpeech-Data/blob/main/Images/Extracted-Tweets-Dumped-in-JSON-file.PNG "Extracted Tweets Dumped in JSON files")


### Phase 2 of ETL: Transform
* Note that the JSON files created are a list of key: value pairs, containing information about the tweet (e.g time of creation, tweet text, tweet sender)
To transform the data, we create a data transform pipeline using the below steps:
1. Create a function `json2df`, which takes a JSON filename as a parameter; to load the JSON files into a list of Python dictionaries
2. Utilize Pandas to convert the dictionaries to Pandas DataFrames
3. Add the converted dataframes to a list named `to_sql`, where each dataframe corresponds to a table in the database
4. The function `json2df` then returns the `to_sql` list, which will be used for the last step of the ETL process

### Phase 3 of ETL: Load

