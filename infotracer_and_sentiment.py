"""# Import"""

import os

from informationtracer import informationtracer
import requests
import mysql.connector

import pandas as pd
import numpy as np
import random

import pytz
from pytz import timezone
from datetime import datetime
from datetime import date
from datetime import datetime, timedelta

from pysentimiento.preprocessing import preprocess_tweet
from pysentimiento import create_analyzer


import itertools

import re
import string
from nltk.stem.snowball import SnowballStemmer
import nltk
from gensim import corpora, models



"""# ETL for infotracer table"""

def convert_time(column):

##############################################################################
##  This function takes the datetime column of any dataframe 
##  and convert utc time to mexico time.
##  This function is part of another function. DO NOT run directly.
##############################################################################

  column=column.apply(lambda x: pytz.utc.localize(x))
  #utc to mexico time
  mexico_tz = timezone('America/Mexico_City')
  convert_timestamp = lambda x: x.astimezone(mexico_tz).replace(tzinfo=None)
  column = column.apply(convert_timestamp)
  return column

def generate_infotracer_table(start_date,end_date,query_dict,update_db=True):

########################################################################################
##  This function query data using information tracer, store in df, convert time and insert
##  data into information tracer table. It returns a df of qury result. This result will be used for sentiment analysis.
##  This function is part of another function. DO NOT run directly.
########################################################################################

  # information tracer token
  with open(os.path.expanduser('~/infotracer_token.txt'), 'r') as f:
      your_token = f.read()

  # query with information tracer api
  df=[]

  for candidate, query in query_dict.items():
    id_hash256 = informationtracer.trace(query=query, token=your_token, start_date=start_date, end_date=end_date, skip_result=True)
    url = "https://informationtracer.com/api/v1/result?token={}&id_hash256={}".format(your_token, id_hash256)
    results = requests.get(url).json() #will get json for all data of keyword, results is a dictionary

    for pf in ['facebook','twitter','instagram','youtube']:
      platform_data=pd.DataFrame(results['posts'][pf], columns=['d','i','n','t'])
      platform_data['platform']=pf
      platform_data['candidate_name']=candidate
      platform_data['t']=pd.to_datetime(platform_data['t'])
      df.append(platform_data)

  df=pd.concat(df)
  df=df.rename(columns={'d':'text','i':'num_interaction','n':'username','t':'datetime'})

  # convert timezone
  df['datetime'] = convert_time(df['datetime'])
  df=df.drop_duplicates()

  print('#################################################')
  print('the shape of infotracer table is:',df.shape)
  print('#################################################')

  if update_db==True:
  
    #read db configs
    with open(os.path.expanduser('~/db_info.txt'), 'r') as f:
        lines = f.readlines()

    localhost = lines[0].strip()
    username = lines[1].strip()
    pw = lines[2].strip()

    #connect to database
    mydb = mysql.connector.connect(
      host=localhost,
      user=username,
      password=pw
    )

    mycursor = mydb.cursor()
    # select database to modify
    mycursor.execute("use dashboard")

    # insert to db
    infotracer_data = df.apply(tuple, axis=1).tolist()
    query="insert into infotracer (text,num_interaction,username,datetime,platform,candidate_name) Values(%s,%s,%s,%s,%s,%s);"
    mycursor.executemany(query,infotracer_data)

    mydb.commit()
    # return query result
  return df

"""# ETL data for sentiment table

## youtube comment functions
"""

##############################################################################
##  NOTE: register youtube API key!
## 
##  Follow instruction here: 
##  https://developers.google.com/youtube/registering_an_application
##
##  One key is enough, more keys can speed up the data collection process
#############################################################################

# read a dictionary of names and values of youtube api key 
API_KEY= {}

with open(os.path.expanduser('~/youtube_tokens.txt'), 'r') as f:
    for line in f:
        key, value = line.strip().split(',')
        API_KEY[key] = value

API_KEY = list(API_KEY.values()) # list of tokens

def search_replies(comment_id):
    headers = {
        'Accept': 'application/json',
    }    

    params = (
        ('part', 'snippet,id'),
        ('parentId', comment_id),
        ('key', random.choice(API_KEY)),
        ('maxResults', '100')        
    )
    
    results = []
    response = requests.get('https://www.googleapis.com/youtube/v3/comments', headers=headers, params=params)
    
    print("Youtube response code --> ", response.status_code)
    if int(response.status_code) > 300:
        print(response.text)
        print('Youtube API v3/videos returns non 200 status code, something is very wrong')
        return results

    results += [item for item in response.json()['items']]
#     print(results)
    nextPageToken = response.json().get('nextPageToken', None)
    
    while nextPageToken:
        print(nextPageToken)
        params = (
            ('part', 'snippet,id'),
            ('parentId', comment_id),
            ('key', random.choice(API_KEY)),
            ('maxResults', '100'),
            ('pageToken', nextPageToken)
        )
        
        response = requests.get('https://www.googleapis.com/youtube/v3/comments', headers=headers, params=params)
        print("Youtube response code --> ", response.status_code)
        if int(response.status_code) > 300:
            print(response.text)
            print('Youtube API v3/videos returns non 200 status code, something is very wrong')
            break
    
        results += [item for item in response.json()['items']]
        nextPageToken = response.json().get('nextPageToken', None)
        
    return results




def search_comments(video_id):        
    headers = {
        'Accept': 'application/json',
    }    

    params = (
        ('part', 'snippet,replies'),
        ('videoId', video_id),
        ('key', random.choice(API_KEY)),
        ('maxResults', '100')        
    )
    
    results = []
    response = requests.get('https://www.googleapis.com/youtube/v3/commentThreads', headers=headers, params=params)
    print("Youtube response code --> ", response.status_code)
    if int(response.status_code) > 300:
        print(response.text)
        print('Youtube API v3/videos returns non 200 status code, something is very wrong')
        return results

    results += [item for item in response.json()['items']]
    nextPageToken = response.json().get('nextPageToken', None)
    
    while nextPageToken:
        print(nextPageToken)
        params = (
            ('part', 'snippet,replies'),
            ('videoId', video_id),
            ('key', random.choice(API_KEY)),
            ('maxResults', '100'),
            ('pageToken', nextPageToken)
        )
        
        response = requests.get('https://www.googleapis.com/youtube/v3/commentThreads', headers=headers, params=params)
        print("Youtube response code --> ", response.status_code)
        if int(response.status_code) > 300:
            print(response.text)
            print('Youtube API v3/videos returns non 200 status code, something is very wrong')
            break
    
        results += [item for item in response.json()['items']]
        nextPageToken = response.json().get('nextPageToken', None)
    
    for i in results:
        if i['snippet']['totalReplyCount'] > 5:
            print('hydrating more comments...')
            i['replies']['comments'] = search_replies(i['id'])
            print(len(i['replies']['comments']))

    # save results    
    return results

def convert_time_ytb(column):
##############################################################################
##  This function takes the datetime column of any dataframe 
##  and convert utc time to mexico time. For ytb comment only.
##  This function is part of another function. DO NOT run directly.
##############################################################################

  #utc to mexico time
  mexico_tz = timezone('America/Mexico_City')
  convert_timestamp = lambda x: x.astimezone(mexico_tz).replace(tzinfo=None)
  column = column.apply(convert_timestamp)
  return column

"""## youtube query"""

def query_youtube_comment(start_date,end_date,query_dict):
##############################################################################
##  This function use information tracer result to query for youtube comment, 
##  and convert utc time to mexico time.
##  This function is part of another function. DO NOT run directly.
##############################################################################

  # information tracer token
  with open(os.path.expanduser('~/infotracer_token.txt'), 'r') as f:
      your_token = f.read()

  # get video id from information tracer source data
  videoId_dic={}
  for candidate, query in query_dict.items():
    id_hash256 = informationtracer.trace(query=query, token=your_token, start_date=start_date, end_date=end_date,skip_result=True)
    url="https://informationtracer.com/loadsource?source={}&id_hash256={}&token={}".format('youtube', id_hash256, your_token)
    results=requests.get(url).json()
    videoId_dic[candidate] = [data['id']['videoId'] for data in results] 
  
  # query comments using youtube api
  ytbcomment_df=[]
  for candidate, videoId_list in videoId_dic.items():
    comment=[]
    username=[]
    likeCount=[]
    date=[]
    for vid in videoId_list:
      if vid!=[]:
        result=search_comments(vid)
        for i in np.arange(0,len(result),1):
          comment.append(result[i]['snippet']['topLevelComment']['snippet']['textDisplay'])
          username.append(result[i]['snippet']['topLevelComment']['snippet']['authorDisplayName'])
          likeCount.append(result[i]['snippet']['topLevelComment']['snippet']['likeCount'])
          date.append(result[i]['snippet']['topLevelComment']['snippet']['publishedAt'])
      ytbcomment= pd.DataFrame({'text': comment, 'num_interaction': likeCount, 
                      'username': username,'datetime': date})
      ytbcomment['platform']='youtube comment'
      ytbcomment['candidate_name']=candidate
      ytbcomment['datetime']=pd.to_datetime(ytbcomment['datetime'])
      ytbcomment_df.append(ytbcomment)
  ytbcomment_df=pd.concat(ytbcomment_df)
  

  # convert timezone
  if ytbcomment_df.empty==False:
    ytbcomment_df['datetime'] = convert_time_ytb(ytbcomment_df['datetime'])
    ytbcomment_df=ytbcomment_df.drop_duplicates()

  print('#################################################')
  print('the shape of youtube comment is:',ytbcomment_df.shape)
  print('#################################################')
  # return query result
  return ytbcomment_df

"""## text process functions"""

# remove \n and ...
def remove_n(df):
  df['text'] = df['text'].str.replace(r'\n|\n.', '', regex=True)
  df['text'] = df['text'].str.replace(r'\.{2,}', '.', regex=True)
  return df
  
# parse
def parse(df):
  print('#################################################')
  print('the shape of merged df is:',df.shape)
  print('start parsing')
  print('#################################################')
  df=remove_n(df)
  df=df.drop_duplicates()
  # new_df = pd.DataFrame(columns=df.columns)
  parsed_df=[]
  # iterate over each row in the original dataframe
  for index, row in df.iterrows():
    print('parsing row', index)
    # split the "text" column value into a list of sentences
    sentences = row['text'].split('. ')  # assuming sentences are separated by ". "
    new_df=pd.DataFrame({
            'text': sentences,
            'num_interaction': row['num_interaction'],
            'username':row['username'],
            'datetime': row['datetime'],
            'platform': row['platform'],
            'candidate_name': row['candidate_name']            
    })
    parsed_df.append(new_df)

  parsed_df=pd.concat(parsed_df)
  return parsed_df

# Define a function to restore spanish accents
def replace_special_chars(text):
  text = re.sub(r'\\u([\da-fA-F]{4})', lambda m: chr(int(m.group(1), 16)), text)
  # Remove punctuation
  text = text.translate(str.maketrans('', '', string.punctuation))
  text = ''.join([i for i in text if not i.isdigit()])
  # # Create a SnowballStemmer for Spanish
  # stemmer = SnowballStemmer('spanish')
  # # Apply stemming
  # words = text.split()
  # stemmed_words = [stemmer.stem(word) for word in words]
  # return ' '.join(stemmed_words)
  return text

def text_process(df):
  # restore accent
  df['text'] = df['text'].apply(replace_special_chars)
  # process tweet is a func from pysentimiento
  df['processed_text'] = df['text'].apply(preprocess_tweet)

  # other steps

  return df

"""## sentiment analysis function"""

# do sentiment analysis in batches
def sent_analyze(df):
  batch=512
  analyzer = create_analyzer(task="sentiment", lang="es", batch_size=batch)

  text=df['processed_text'].tolist() #list of texts
  label=[]
  pos_prob=[]
  neu_prob=[]
  neg_prob=[]
  for i in range(0, len(text), batch):
    analyze_result=analyzer.predict(text[i:i+batch])

    label+=[r.output for r in analyze_result]
    pos_prob+=[r.probas['POS'] for r in analyze_result]
    neu_prob+=[r.probas['NEU'] for r in analyze_result]
    neg_prob+=[r.probas['NEG'] for r in analyze_result]
    print("Batch {} done".format(int(i/512)))

  df['label']=label
  df['positive']=pos_prob
  df['neutral']=neu_prob
  df['negative']=neg_prob
  return df

def generate_infotracer_and_sentiment_table(start_date,end_date,ytb_end_date,query_dict, update_db=True):

###########################################################################
## This function generates both infotracer and sentiment table.
## It calls information tracer and ytb comment function within it,
## merge result, process text and conduct sentiment analysis.
###########################################################################


  # information tracer token
  with open(os.path.expanduser('~/infotracer_token.txt'), 'r') as f:
      your_token = f.read()

  # get information tracer query result (also insert to db)
  df=generate_infotracer_table(start_date=start_date,end_date=end_date,query_dict=query_dict,update_db=update_db)

  #get youtube comment query result
  ytbcomment_df=query_youtube_comment(start_date=start_date,end_date=ytb_end_date,query_dict=query_dict)

  # merge results
  # text for sentiment table: everything in df + used youtube raw data to extract comment
  sentiment_df=pd.concat([ytbcomment_df,df])
  sentiment_df=sentiment_df.reset_index(drop=True)

  #parse post into sentences
  sentiment=parse(sentiment_df)

  #################################
  ## text processing
  #################################
  sentiment=text_process(sentiment)

  # sentiment calculation

  full_sentiment=sent_analyze(sentiment)
  
  if update_db==True:

    #read db configs
    with open(os.path.expanduser('~/db_info.txt'), 'r') as f:
          lines = f.readlines()

    localhost = lines[0].strip()
    username = lines[1].strip()
    pw = lines[2].strip()

    #connect to database
    mydb = mysql.connector.connect(
      host=localhost,
      user=username,
      password=pw
    )

    mycursor = mydb.cursor()
    # select database to modify
    mycursor.execute("use dashboard")

    # commit full sentiment to sentiment table in db
    sent_data = full_sentiment.apply(tuple, axis=1).tolist()
    query="insert into sentiment (text,num_interaction,username,datetime,platform,candidate_name,processed_text,label, positive, neutral, negative) Values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);" 
    mycursor.executemany(query,sent_data)

    mydb.commit()

  return