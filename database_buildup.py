# -*- coding: utf-8 -*-

# !pip install mysql-connector-python
# !pip install informationtracer
# !pip install pysentimiento

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
from wordcloud import WordCloud

"""# Create database and table"""

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
# create a databse called "dashboard"
#mycursor.execute("CREATE DATABASE dashboard")

# select database to modify
mycursor.execute("use dashboard")

# delete table
# mycursor.execute("drop table if exists infotracer")
# mycursor.execute("drop table if exists sentiment")
# mycursor.execute("drop table if exists wordcloud")
# mycursor.execute("drop table if exists network")
# mycursor.execute("drop table if exists helper")

# create table
# mycursor.execute("create table infotracer (candidate_name varchar(255), text MediumTEXT, username varchar(255), num_interaction int, datetime timestamp, platform varchar(255) )")
# mycursor.execute("create table sentiment (text MediumTEXT, processed_text MediumTEXT, positive float, neutral float, negative float, label varchar(255),candidate_name varchar(255), platform varchar(255), username varchar(255), num_interaction int, datetime timestamp )")
# mycursor.execute("create table wordcloud (word varchar(255), weight float, candidate_name varchar(255),platform varchar(255))")
# mycursor.execute("create table network (source varchar(255), target varchar(255), weight float, source_cat varchar(255), target_cat varchar(255), source_datetime timestamp, target_datetime timestamp, candidate_name varchar(255))")
# mycursor.execute("create table helper (datetime timestamp,candidate_name varchar(255),label varchar(255),platform varchar(255))")

"""#ELT helper table"""

def generate_helper_table(start_date,end_date,query_dict,update_db=True):
  
##############################################################################
##  This function generates a helper table that is used to join with other table to fill in time gaps.
##  Records are inserted to table within function.
##############################################################################

  # Define platforms and labels
  platforms = ['facebook','twitter','instagram','youtube','youtube comment']
  labels = ['POS','NEU','NEG']
  candidate_name=list(query_dict.keys())
  # Generate date range
  date_range = pd.date_range(start=start_date, end=end_date)

  # Generate combinations of platform and label for each date
  date_platform_label = list(itertools.product(date_range, platforms, labels,candidate_name))

  # Create DataFrame
  helper_df = pd.DataFrame(date_platform_label, columns=["datetime", "platform", "label",'candidate_name'])


  if update_db==True:
    # Insert to db
    helper_data = helper_df.apply(tuple, axis=1).tolist()
    # change order of column to fit df
    query="insert into helper (datetime,platform,label, candidate_name) Values(%s,%s,%s,%s);" 
    mycursor.executemany(query,helper_data)
    mydb.commit()

  return

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
    id_hash256 = informationtracer.trace(query=query, token=your_token, start_date=start_date, end_date=end_date)
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

API_KEY = list(API_KEY.values())

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
    # commit full sentiment to sentiment table in db
    sent_data = full_sentiment.apply(tuple, axis=1).tolist()
    query="insert into sentiment (text,num_interaction,username,datetime,platform,candidate_name,processed_text,label, positive, neutral, negative) Values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);" 
    mycursor.executemany(query,sent_data)

    mydb.commit()

  return

"""# ETL data for wordcloud

"""

def clean_text(sentence):
  # all lower-case
  sentence = sentence.lower()
  # remove punctuations 
  sentence = re.sub(r"[,.¡¿\"@#$%^&*(){}?/;:<>+=-]", "", sentence)
  # remove numbers
  sentence = re.sub(r'\d+', '', sentence)
  # remove leading/trailing whitespace
  sentence = sentence.strip()
  
  return sentence

def generate_wordcloud_table():
  #query for data in last 14 days
  wc_query= "SELECT processed_text, platform, candidate_name, datetime FROM sentiment WHERE datetime >= DATE_SUB(NOW(), INTERVAL 14 DAY) "
  word_cloud_df= pd.read_sql_query(wc_query, mydb)
  
  #process text
  word_cloud_df['processed_text']=word_cloud_df['processed_text'].apply(lambda x: clean_text(x))


  # stop words
  
  with open(os.path.expanduser('~/spanish.txt'), 'r') as f:
    spanish_stopwords = [line.strip() for line in f]
  stop_words = set(spanish_stopwords)
  stop_words.add('emoji') 
  stop_words.add('http')
  stop_words.add('youtube')
  stop_words.add('twitter')
  stop_words.add('instagram')
  stop_words.add('facebook')

  # generate wordcloud df
  wc=[]
  for platform in word_cloud_df['platform'].unique():
    for candidate in word_cloud_df['candidate_name'].unique():
      all_text = ' '.join(word_cloud_df[(word_cloud_df['platform']==platform)&(word_cloud_df['candidate_name']==candidate)]['processed_text'])
      if all_text=='':
        all_text='NADA'
        print(platform, candidate, all_text)
      wordcloud = WordCloud(stopwords=stop_words).generate(all_text)
      # Get list of words in wordcloud
      word_list = list(wordcloud.words_.keys())
      word_weights = list(wordcloud.words_.values())
      sub_wc=pd.DataFrame({'word':word_list, 'frequency':word_weights,'platform': platform,'candidate_name':candidate})
      wc.append(sub_wc)

  wc=pd.concat(wc)
  print('shape of wordcloud is',wc.shape)

  # empty the table
  print('empty the table')
  mycursor.execute(f'DELETE FROM wordcloud')
  mydb.commit()

  # insert to db last 14 days data
  print('start inserting')
  wc_data = wc.apply(tuple, axis=1).tolist()
  query="insert into wordcloud (word,weight, platform, candidate_name) Values(%s,%s,%s,%s);" 
  mycursor.executemany(query,wc_data)
  print('done')

  mydb.commit()

  return

"""# ETL data for network"""

def generate_network_table(start_date,end_date,query_dict, update_db=True):
  

  # information tracer token
  with open(os.path.expanduser('~/infotracer_token.txt'), 'r') as f:
      your_token = f.read()

  for candidate, query in query_dict.items():
    ## extract data
    id_hash256 = informationtracer.trace(query=query, token=your_token, start_date=start_date, end_date=end_date)
    url = "https://informationtracer.com/cross_platform/{}/interaction_network_{}.json".format(id_hash256[:3], id_hash256)

    network_json = requests.get(url).json()

    ## construct df
    node_id_to_category = {}
    node_id_to_name={}
    node_id_to_datetime={}
    node_id_to_platform={}

    ## iterate all nodes to save node type
    for node in network_json['nodes']:
      node_id_to_category[node['id']] = node['type']
      node_id_to_name[node['id']]=node['name']
      node_id_to_datetime[node['id']]=node['timestamp']
      node_id_to_platform[node['id']]=node['platform']

    ## iterate all links
    source=[]
    target=[]
    weight=[]
    source_cat=[]
    target_cat=[]
    source_datetime=[]
    target_datetime=[]

    for link in network_json['links']:
      source_id = link['source']
      target_id = link['target']
      source_datetime.append(node_id_to_datetime[source_id])
      target_datetime.append(node_id_to_datetime[target_id])

      source_cat.append(node_id_to_category[source_id])
      target_cat.append(node_id_to_category[target_id])

      # to distinguish user with same name on different platform
      if node_id_to_category[source_id]=='user':
        source.append((node_id_to_name[source_id]+' '+node_id_to_platform[source_id]))
      else: 
        source.append(node_id_to_name[source_id])
      
      if node_id_to_category[target_id]=='user':
        target.append((node_id_to_name[target_id]+' '+node_id_to_platform[target_id]))
      else:
        target.append(node_id_to_name[target_id])
      
      weight.append(link['weight'])


    nw_df = pd.DataFrame({
        'source': source,
        'target': target,
        'weight': weight,
        'source_cat': source_cat,
        'target_cat': target_cat,
        'source_datetime': source_datetime,
        'target_datetime': target_datetime
    })
    nw_df['source_datetime']=pd.to_datetime(nw_df['source_datetime'])
    nw_df['target_datetime']=pd.to_datetime(nw_df['target_datetime'])
    nw_df['source_datetime']=convert_time(nw_df['source_datetime'])
    nw_df['target_datetime']=convert_time(nw_df['target_datetime'])

    nw_df['candidate_name']=candidate
    if update_db==True:
      ##### insert into mysql table source, target, weight, source_category, target_category
      nw_data=nw_df.apply(tuple, axis=1).tolist()
      query="insert into network (source, target, weight, source_cat, target_cat, source_datetime, target_datetime, candidate_name) Values(%s,%s,%s,%s,%s,%s,%s,%s);"
      mycursor.executemany(query,nw_data)

      mydb.commit()

  return



"""# Historical"""

# # infotracer and sentiment: add historical data
# generate_infotracer_and_sentiment_table(start_date="2023-01-01",end_date="2023-04-08",ytb_end_date="2023-04-08")
# # network: add historical data
# generate_network_table(start_date="2023-01-01",end_date="2023-04-08")
# # helper: add historical data
# generate_helper_table(start_date="2023-01-01",end_date="2023-04-08")

"""# Daily update"""

query_dict={'Manolo Jiménez Salinas':'"Manolo Jiménez Salinas" OR manolojim OR manolojimenezs OR Manolo.Jimenez.Salinas',
      'Armando Guadiana Tijerina':'"Armando Guadiana Tijerina" OR aguadiana OR armandoguadianatijerina OR ArmandoGuadianaTijerina',
'Ricardo Mejia Berdeja':'"Ricardo Mejia Berdeja" OR RicardoMeb OR ricardomeb OR RicardoMejiaMx',
    'Lenin Perez Rivera':'"Lenin Perez Rivera" OR leninperezr OR leninperezr OR leninperezr'
}

# helper: daily update
today = datetime.now(pytz.timezone('America/Mexico_City')).date()
generate_helper_table(start_date=today, end_date=today,query_dict=query_dict, update_db=False)
print('helper table done')

# infotracer and sentiment: daily update
today = datetime.now(pytz.timezone('America/Mexico_City')).date().strftime('%Y-%m-%d')
tomorrow=(datetime.now(pytz.timezone('America/Mexico_City')).date() + timedelta(days=1)).strftime('%Y-%m-%d')
generate_infotracer_and_sentiment_table(start_date=today, end_date=today, ytb_end_date=tomorrow,query_dict=query_dict,update_db=False)
print('infotracer and sentiment table done')

# wordcloud: daily update
generate_wordcloud_table()
print('wordcloud table done')

# network: daily update
generate_network_table(start_date=today, end_date=today,query_dict=query_dict,update_db=False)
print('network table done')
