from json.decoder import JSONDecodeError
import time
import json
import numpy as np
import pandas as pd
import sqlalchemy
from sshtunnel import SSHTunnelForwarder
from sqlalchemy import create_engine
from sqlalchemy import MetaData
from sqlalchemy import Table
import glob
import os
import datetime
from datetime import timedelta

#### ETL Script #####

# start script execution timer
start = time.time()

def list_col_to_str_col(df):
    """
    if a column in the dataframe is a list
    convert it into str so it can be stored in the database
    Parameters
    ----------
        df: the dataframe to be converted
    Returns
    -------
        None
    """

    for col in df.columns:
        if type(df[col].iloc[0]) is list:
            df[col] = df[col].apply(str)


def expand_dict(df):
    """
    expand a two column dataframe of which the second column
    contains dictionarys
    Parameters
    ----------
        df: 2-column pandas dataframe
    Returns
    -------
        df_with_id: multi-column pandas dataframe
            the number of columns after expantion is equal to the number
            of keys in the dictionary
    """
    ID = df.iloc[:,0]
    series = df.iloc[:,1].tolist()
    df_from_series = pd.DataFrame(series, index=df.index)
    df_with_id = pd.concat([ID, df_from_series], axis=1)

    return df_with_id


def drop_empty_list(df):
    """
    replace the empty lists in a two-column dataframe into np.nan
    so they can be dropped using df.dropna() method
    Parameters
    ----------
        df: two-column pandas dataframe
    Returns
    -------
        df_dropped: with all the empty lists dropped
    """

    replace_nan = lambda x: np.nan if len(x)==0 else x
    col = df.iloc[:,1].copy()
    new_col = col.apply(replace_nan)
    df_replaced = pd.concat([df.iloc[:,0], new_col], axis=1)

    df_dropped = df_replaced.dropna()
    return df_dropped


def merge_dicts(df):
    """
    when the other column of the dataframe contains a list of dictionarys
    with the same key and value pairs
    we merge the list to a single dictionary
    Parameters
    ----------
        df: input dataframe
    Returns
    -------
        df_merged_w_id
    """
    series = df.iloc[:,1].copy().tolist()
    merged_series = []
    for dict_lst in series:
        dict_1 = dict_lst[0]
        total_dict = {key:[] for key in list(dict_1.keys())}
        for dit in dict_lst:
            for key in list(dict_1.keys()):
                total_dict[key].append(dit[key])
        merged_series.append(total_dict)
    df_merged = pd.DataFrame(merged_series, index=df.index)
    df_merged_w_id = pd.concat([df.iloc[:,0], df_merged], axis=1)
    
    return df_merged_w_id


def json2df(filename):
    """
    load json file into a list of dictionarys
    
    Parameters
    ----------
        filename: string
            the directory of the json file
    
    Returns
    -------
        to_sql: list of pandas dataframes
            each dataframe corresponds to a table in the database
    """
    
    
    """
    First section:
        load the tweets from the json file
        then coarsely split them into base tweets and dict tweets
    """
    
    # convert from JSON to Python object
    
    
    # initialize a list for all the json lines
    tweets = []
    
    # initialize a dictionary for all the dataframes
    to_sql = {}

    """
    # Display Number of lines
    with open(filename, 'r') as f:
        len_file = len(f.readlines())
        print(len_file)
    """
    
    # load the json file into a list
    with open(filename, 'r') as f:
        for line in f:
            try:
                if line != '\n':
                    tweet = json.loads(line)
                    tweets.append(tweet)
            
            except JSONDecodeError:
                json_error_files.append(filename)
                print(filename + ' :JSONDecodeError: File Probably Having an Incomplete Write')
                #del tweets[-1]
                #print(len(tweets))
                break

    
    # convert the list of lines into a dataframe
    tweets_df = pd.DataFrame(tweets)
    tweets_df.rename(columns={'id':'tweet_id',
                              'id_str':'tweet_id_str'}, inplace=True)
    
    #print(tweets_df.columns)
    # specify the columns that have to be stored in dicts
    to_dicts = ['tweet_id',
                'coordinates',
                'entities',
                'extended_entities',
                'extended_tweet',
                'place',
                'quoted_status',
                'quoted_status_permalink',
                'retweeted_status',
                'user']
    
    # specify the columns to drop
    to_drop = [ 'contributors',
                'display_text_range',
                'coordinates',
                'entities',
                'extended_entities',
                'extended_tweet',
                'geo',
                'place',
                'quoted_status',
                'quoted_status_permalink',
                'retweeted_status',
                'user']
    
    
    # divide the raw tweets into normal part (without dicts)
    # and dict part (needs to be saved into multiple tables)
    #try:
    tweets_dicts = tweets_df[to_dicts]
    tweets_non_dicts = tweets_df.drop(to_drop, axis=1)
    
    # add the base tweets into the dictionary
    to_sql['base_tweets'] = tweets_non_dicts
    
    # seperate the columns in the dicts dataframe into multiple dataframes
    tweets_2dict_lst = []
    for idx in range(1, len(to_dicts)):
        tweets_2dict_lst.append(tweets_dicts[['tweet_id', to_dicts[idx]]].dropna())
    
    """
    Second section:
        for each dataframes in the tweets_2dict_lst
        flatten them into a cleaner format of dataframes
    """
    
    """
    2.1 coordinates
    """
    coor_df = tweets_2dict_lst[0]
    
    if len(coor_df)==0:
        
        to_sql['coordinates'] = pd.DataFrame()
        
    else:
    
        coor_df = expand_dict(coor_df)
        
        list_col_to_str_col(coor_df)
        
        to_sql['coordinates'] = coor_df
    
    """
    2.2 entities
    """
    # get the entities first
    entities_df = expand_dict(tweets_2dict_lst[1])
    
    # get hashtags and user_mentions
    hashtags_df = drop_empty_list(entities_df[['tweet_id', 'hashtags']])
    user_mentions_df = drop_empty_list(entities_df[['tweet_id', 'user_mentions']])
    
    # merge them
    hashtags_df = merge_dicts(hashtags_df)
    user_mentions_df = merge_dicts(user_mentions_df)
    
    # convert list object to str
    list_col_to_str_col(hashtags_df)
    list_col_to_str_col(user_mentions_df)
    
    # add into list
    to_sql['hashtags'] = hashtags_df.rename({'text':'hashtag'}, axis=1)
    to_sql['user_mentions'] = user_mentions_df
    
    """
    2.3-2.5 extended tweet, quoted tweet, retweeted
    """
    
    # use the json indexing to get the useful information
    # initialize each dictionary
    extended_tweet = {'tweet_id':[],
                      'full_text':[],
                      'user_mentions':[],
                      'extended_hashtags':[]}
    
    quoted_tweet = {'tweet_id':[],
                    'quoted_id':[],
                    'quoted_text':[],
                    'quoted_hashtags':[]}
    
    quoted_user = {'tweet_id':[],
                   'quoted_user':[]}
    
    retweeted_tweet = {'tweet_id':[],
                       'retweeted_id':[],
                       'retweeted_text':[],
                       'retweeted_hashtags':[]}
    
    retweeted_user = {'tweet_id':[],
                      'retweeted_user':[]}
    
    
    with open(filename, 'r') as file:
        for line in file:

            try: 
                tweet = json.loads(line)
                
                # get info for extended tweets
                if 'extended_tweet' in tweet.keys():
                    extended_tweet['tweet_id'].append(tweet['id'])
                    extended_tweet['full_text'].append(tweet['extended_tweet']['full_text'])
                    user_mentions = [dit['id'] for dit in tweet['extended_tweet']['entities']['user_mentions']]
                    extended_tweet['user_mentions'].append(user_mentions)
                    extended_hashtags = [dit['text'] for dit in tweet['extended_tweet']['entities']['hashtags']]
                    extended_tweet['extended_hashtags'].append(extended_hashtags)
                    
                # get info for quoted tweets
                if 'quoted_status' in tweet.keys():
                    quoted_tweet['tweet_id'].append(tweet['id'])
                    quoted_tweet['quoted_id'].append(tweet['quoted_status']['id'])
                    quoted_tweet['quoted_text'].append(tweet['quoted_status']['text'])
                    quoted_hashtags = [dit['text'] for dit in tweet['quoted_status']['entities']['hashtags']]
                    quoted_tweet['quoted_hashtags'].append(quoted_hashtags)
                    
                    # get info for quoted users
                    
                    quoted_user['tweet_id'].append(tweet['id'])
                    quoted_user['quoted_user'].append(tweet['quoted_status']['user'])
                    
                # get info for retweeted tweets
                if 'retweeted_status' in tweet.keys():
                    retweeted_tweet['tweet_id'].append(tweet['id'])
                    retweeted_tweet['retweeted_id'].append(tweet['retweeted_status']['id'])
                    retweeted_tweet['retweeted_text'].append(tweet['retweeted_status']['text'])
                    retweeted_hashtags = [dit['text'] for dit in tweet['retweeted_status']['entities']['hashtags']]
                    retweeted_tweet['retweeted_hashtags'].append(retweeted_hashtags)
                    
                    # get info for quoted users
                    
                    retweeted_user['tweet_id'].append(tweet['id'])
                    retweeted_user['retweeted_user'].append(tweet['retweeted_status']['user'])
                    
            except JSONDecodeError:
                print(filename + ' :JSONDecodeError Level 2')
                #del tweets[-1]
                #print(len(tweet))
                break

        extended_tweet_df = pd.DataFrame(extended_tweet)
        quoted_tweet_df = pd.DataFrame(quoted_tweet)
        quoted_user_df = pd.DataFrame(quoted_user)
        retweeted_tweet_df = pd.DataFrame(retweeted_tweet)
        retweeted_user_df = pd.DataFrame(retweeted_user)
        
    # handle the new dataframes
    # expand two user dataframes
    quoted_user_df = expand_dict(quoted_user_df)
    retweeted_user_df = expand_dict(retweeted_user_df)
    
    # convert list to str
    list_col_to_str_col(extended_tweet_df)
    list_col_to_str_col(quoted_tweet_df)
    list_col_to_str_col(retweeted_tweet_df)

    quoted_user_df.drop(quoted_user_df.columns[40], axis=1, inplace=True)
    retweeted_user_df.drop(retweeted_user_df.columns[40], axis=1, inplace=True)

    """
    #### Examining Dfs
    print('Quoted User Df: ' + str(type(quoted_user_df)))
    print('EXTENDED TWEET DF')
    print(extended_tweet_df.columns)
    print('QUOTED TWEET DF')
    print(quoted_tweet_df.columns)
    print('QUOTED USER DF')
    print(quoted_user_df.columns)
    print('RETWEETED TWEET DF')
    print(retweeted_tweet_df.columns)
    print('RETWEETED USER DF')
    print(retweeted_user_df.columns)
    """
    #print('QUOTED USER DF')

    #print(quoted_user_df.columns)
    #print('Quoted User Df: ' + str(quoted_user_df.shape))

    # quoted user cols to drop
    #print(quoted_user_df.columns[23:])
    
    # delete cols
    quoted_user_df.drop(quoted_user_df.columns[23:], axis=1, inplace=True)
    
    # add into list
    to_sql['extended_tweets'] = extended_tweet_df
    to_sql['quoted_tweets'] = quoted_tweet_df
    to_sql['quoted_user'] = quoted_user_df
    to_sql['retweeted_tweet'] = retweeted_tweet_df
    to_sql['retweeted_user'] = retweeted_user_df

    #print("Len of quoted user table: " + str(quoted_user_df.shape))
    #print("Len of retweeted user df table: " + str(retweeted_user_df.shape))
    
    """
    2.6 place
    """
    # handle place dataframe
    place_df = tweets_2dict_lst[4]
    
    if len(place_df)==0:
        
        to_sql['place'] = pd.DataFrame()
        
    else:
    
        # expand it first
        place_df = expand_dict(place_df).drop(['attributes'], axis=1)
        
        # note that bounding box column contains dictionarys
        # make it a new dataframe into two columns
        bounding_box_lst = place_df.loc[:, 'bounding_box'].copy().tolist()
        bounding_box_df = pd.DataFrame(bounding_box_lst,
                                       index=place_df.index)
        bounding_box_df.rename(columns={'coordinates': 'bounding_box_coordinates',
                                        'type': 'bounding_box_type'})
        
        # then merge it back and drop the original one
        place_df = pd.concat([place_df, bounding_box_df], axis=1)
        place_df.drop(['bounding_box'], axis=1, inplace=True)
        
        # convert list to str
        list_col_to_str_col(place_df)
        
        # add to list
        to_sql['place'] = place_df
    
    """
    2.7 user
    """
    # expand it
    user_df = expand_dict(tweets_2dict_lst[8])
    to_keep = ['tweet_id',
               'created_at',
               'description',
               'favourites_count',
               'followers_count',
               'geo_enabled',
               'id',
               'lang',
               'location',
               'url',
               'verified',
               'friends_count']
    
    # add to list
    to_sql['tweet_user'] = user_df[to_keep].copy()

    #print(type(to_sql))
    
    """
    Third section
        parse datetime object
    """
    # for each dataframe in the list
    """
    for df_name, df in to_sql.items():
        if len(df) != 0:
            if 'created_at' in df.columns:
                df.created_at = pd.to_datetime(df.created_at)
            df.drop_duplicates(inplace=True)
    """

    """
    print("Number of df's (Tables): " + str(len(to_sql)))
    for table in to_sql:
        print(table)
    """
        
    return to_sql




# to postgresql db
engine = create_engine('postgresql://imwendwa:db$n6kTs@localhost:5432/police_and_elections_db_prod')
con=engine

"""
### Connect to dB via SSH Tunnel
creds = json.load(open("/CarinaNebula/twitterScraping/version2/server_files/test_db/db_cred.json", 'r'))

def connect_sqlalc():
    try:
        print('Connecting to the PostgreSQL Database...')
    
        ssh_tunnel = SSHTunnelForwarder(
                (creds["SSH_HOST"], 694),
                ssh_username=creds["PG_UN"],
                ssh_private_key= '/CarinaNebula/twitterScraping/version2/db-ssh-cert/id_rsa',
                ssh_private_key_password= creds["SSH_PKEY"],
                remote_bind_address=(creds["DB_HOST"], 5432)
            )
        
        ssh_tunnel.start()

        engine = create_engine('postgresql://{user}:{password}@{host}:{port}/{db}'.format(
                 user=creds["PG_UN"],
                 password=creds["PG_DB_PW"],
                 host=creds["LOCALHOST"],
                 port=ssh_tunnel.local_bind_port,
                 db=creds["PG_DB_NAME"]
                ))
        print('Connection successful')
    except:
        print('Connection Has Failed...')  
    return engine

# Call Connect to dB function
engine = connect_sqlalc()
con=engine
"""



# list all files in directory
#file_str = '/home/imwendwa/analytics/terrorismTwitterScraping/streaming_json_files/streamed*.json'
#file_str = '/CarinaNebula/twitterScraping/version2/streaming_json_files/test/test_full/streamed*.json'
#file_str = '/CarinaNebula/twitterScraping/version2/server_files/test_db/partial_files/streamed*.json'
#file_lst = glob.glob(file_str)

#list all files in directory
localpath = '/home/imwendwa/analytics/policeAndElectionsTwitterScraping/streaming_json_files/'
#localpath = '/home/imwendwa/analytics/test_db/police/streaming_json_files/'
#localpath = '/CarinaNebula/twitterScraping/version2/streaming_json_files/test/test_full/'
fnames = os.listdir(localpath)

files_processing_list = [] #list of files for processing





#files_processed = [] # list of files successfully processed
sql_error_files = [] # list of files with sql errors during processing
json_error_files = [] # ist of files with json errors during processing


#for file_idx, file_name in enumerate(file_lst):
#    print(type(file_name))
#    to_sql = json2df(file_name)

"""
for file_idx, file_name in enumerate(file_lst):
    print(file_name)
    if file_idx == 0:
        # Drop the table before inserting new values
        option = 'replace'
    else:
        # Insert new values to the existing table
        option = 'append'
"""

current_date_and_time = datetime.datetime.now()
current_date_str = current_date_and_time.strftime('%y%m%d')
current_date_f = datetime.datetime.strptime(current_date_str, '%y%m%d')
current_date_f = datetime.datetime.date(current_date_f)
#print(current_date_f)
#print(type(current_date_f))

#proc_mod_date = current_date_f
proc_mod_date = current_date_f - timedelta(days=1)

"""
##### Specific Day Execution
proc_mod_date = datetime.datetime.strptime('21-12-02', '%y-%m-%d')
proc_mod_date = datetime.datetime.date(proc_mod_date)
#print(proc_mod_date)
"""

# file processing by date
for filename in fnames:
    mfilename = os.path.basename(filename)
    fullpath = localpath + mfilename
    #print(fullpath)
    modDateF = time.strftime('%y%m%d', time.localtime(os.path.getmtime(fullpath)))
    #print(type(modDateF))
    mod_date_f = datetime.datetime.strptime(modDateF, '%y%m%d')
    mod_date_f = datetime.datetime.date(mod_date_f)
    #print(mod_date_f)
    #print(type(mod_date_f))


    if mod_date_f == proc_mod_date:  # if mod date of file == processing mod Date
        files_processing_list.append(fullpath)  # append to the list for processing    
    
option = 'append' #append to dB instead of replace

#print('PROCESSING FILES FOR DATE: ' + str(proc_mod_date))
print('PROCESSING FILES UP TO DATE: ' + str(proc_mod_date))

# ETL Processing of files
for file in files_processing_list:
    print(file)

    # Call Extract, Transform Function
    to_sql = json2df(file)
    

    for df_name, df in to_sql.items():
        #print(file_idx, df_name)

        # Call Load Function
        try:
            df.to_sql(df_name, con, if_exists=option) # Load function
            #files_processed.append(file_name) #add processed file
            #print('Successfully loaded: ' + file_name)
        #except pyscopg2.errors.UndefinedColumn:
        except sqlalchemy.exc.ProgrammingError:
            sql_error_files.append(fullpath)
            print(fullpath + ' :SQL Alchemy Error due to Additional Columns')
            break

        except sqlalchemy.exc.IntegrityError:
            #print(fullpath + ' : Integrity Error')
            #break
            continue


        # end open transaction
        #con.execute('commit')
    
# print number of processed files
#files_num = len(files_processed)
#print('Number of Successfully Processed Files: ' + str(files_num))

print('Number of Files to be processed: ' + str(len(files_processing_list)))
#print number of error files: SQL Alchemy
sql_error_files_num = len(sql_error_files)
print('Number of Files with SQLAlchemy Error (Additional Columns): ' + str(sql_error_files_num))

#print number of error files: SQL Alchemy
json_error_files_num = len(json_error_files)
print('Number of Files with JSON Error (Incomplete Write): ' + str(json_error_files_num))


# stop script execution timer
end = int(time.time() - start)

# Display time of execution
print("Script Finished Executing in : " + str(end / 60) + " Minutes")