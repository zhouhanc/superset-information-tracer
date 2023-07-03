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
# from nltk.stem.snowball import SnowballStemmer
import nltk
# from gensim import corpora, models
from wordcloud import WordCloud


# ETL data for wordcloud


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

def generate_wordcloud_table(query_dict, config, update_db=False):


  # connect to database
  mydb = mysql.connector.connect(
    host=config["db_info"]["localhost"],
    user=config["db_info"]["username"],
    password=config["db_info"]["pw"]
  )


  mycursor = mydb.cursor()
  database_name = config["database_name"] 
  # select database to modify
  mycursor.execute(f"USE {database_name}")


  
  # read time and language config 
  last_n_days = config["generate_wordcloud_table"]["last_n_days"]
  stop_word_file = config["generate_wordcloud_table"]["stop_word_file"]


  #query for data in last_n_days
  # wc_query= f"SELECT processed_text, platform, candidate_name, datetime FROM sentiment WHERE datetime >= DATE_SUB(NOW(), INTERVAL {last_n_days} DAY) "
  wc_query= f"SELECT text, platform, candidate_name, datetime FROM infotracer "

  word_cloud_df= pd.read_sql_query(wc_query, mydb)
  
  #process text
  # word_cloud_df['processed_text']=word_cloud_df['processed_text'].apply(lambda x: clean_text(x))
  word_cloud_df['text']=word_cloud_df['text'].apply(lambda x: clean_text(x))

  # stop words
  
  with open(f'{stop_word_file}.txt', 'r') as f:
    stopwords = [line.strip() for line in f]

  stop_words = set(stopwords)
  stop_words.add('emoji') 
  stop_words.add('http')
  stop_words.add('youtube')
  stop_words.add('twitter')
  stop_words.add('instagram')
  stop_words.add('facebook')

  # generate wordcloud df
  wc=[]
  for platform in word_cloud_df['platform'].unique():
    for candidate in query_dict.keys():
      all_text = ' '.join(word_cloud_df[(word_cloud_df['platform']==platform)&(word_cloud_df['candidate_name']==candidate)]['processed_text'])
      if all_text=='':
        all_text='_nothing_to_show_'
        print(platform, candidate, all_text)
      wordcloud = WordCloud(stopwords=stop_words).generate(all_text)
      # Get list of words in wordcloud
      word_list = list(wordcloud.words_.keys())
      word_weights = list(wordcloud.words_.values())
      sub_wc=pd.DataFrame({'word':word_list, 'frequency':word_weights,'platform': platform,'candidate_name':candidate})
      wc.append(sub_wc)

  wc=pd.concat(wc)
  print('shape of wordcloud is',wc.shape)

  if update_db == True:
    # empty the table
    print('empty the table')
    mycursor.execute(f'DELETE FROM wordcloud')
    mydb.commit()

    # insert to db last_n_days data
    print('start inserting')
    wc_data = wc.apply(tuple, axis=1).tolist()
    query="insert into wordcloud (word, weight, platform, candidate_name) Values(%s,%s,%s,%s);" 
    mycursor.executemany(query,wc_data)
    print('done')

    mydb.commit()
    mydb.close()

  return
