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
# from nltk.stem.snowball import SnowballStemmer
import nltk
# from gensim import corpora, models
from wordcloud import WordCloud

# import functions from other files
from helper import generate_helper_table
from infotracer_and_sentiment import generate_infotracer_and_sentiment_table
from wordcloud_table import generate_wordcloud_table
from network_table import generate_network_table

import json


# read config
with open('config.json', 'r' ,encoding='utf-8') as f:
  config=json.load(f)




#connect to database
mydb = mysql.connector.connect(
  host=config["db_info"]["localhost"],
  user=config["db_info"]["username"],
  password=config["db_info"]["pw"]
)


mycursor = mydb.cursor()
mycursor.execute("SHOW DATABASES")
all_databases = mycursor.fetchall()
print("all available databases:",all_databases)

database_name = config["database_name"] 

if (database_name,) not in all_databases:
  # create a db
  mycursor.execute(f"CREATE DATABASE {database_name}")

  # drop a db
  # mycursor.execute(f"DROP DATABASE {database_name}")

  # select database to modify
  mycursor.execute(f"USE {database_name}")

  # create table and schema
  mycursor.execute("create table infotracer (candidate_name varchar(255), text MediumTEXT, username varchar(255), num_interaction int, datetime timestamp, platform varchar(255) )")
  mycursor.execute("create table sentiment (text MediumTEXT, processed_text MediumTEXT, positive float, neutral float, negative float, label varchar(255),candidate_name varchar(255), platform varchar(255), username varchar(255), num_interaction int, datetime timestamp )")
  mycursor.execute("create table wordcloud (word varchar(255), weight float, candidate_name varchar(255),platform varchar(255))")
  mycursor.execute("create table network (source varchar(255), target varchar(255), weight float, source_cat varchar(255), target_cat varchar(255), source_datetime timestamp, target_datetime timestamp, candidate_name varchar(255))")
  mycursor.execute("create table helper (datetime timestamp,candidate_name varchar(255),label varchar(255),platform varchar(255))")
 
  # delete table
  # mycursor.execute("drop table if exists infotracer")
  # mycursor.execute("drop table if exists sentiment")
  # mycursor.execute("drop table if exists wordcloud")
  # mycursor.execute("drop table if exists network")
  # mycursor.execute("drop table if exists helper")
  # mydb.commit()

else: 
  print(database_name,"exists, skip building")




# start data collection
print("the data collection is for the following candidates:", config["query_dict"].keys())


# use config to choose: historical OR daily + refresh
# each block should be surrounded by try except

if config["date"]["predefined_period"]==True:
  # do historical data collection


  # helper: add historical data
  if config["generate_helper_table"]["run"]==True:
    try:
      generate_helper_table(start_date = config["date"]["start_date"], 
                            end_date = config["date"]["end_date"], 
                            query_dict = config["query_dict"], 
                            config,
                            update_db=config["generate_helper_table"]["update_db"]
                            )
    except Exception as e:
      print(f"An error occurred for helper table when updating historical data: {e}")
  
  # infotracer and sentiment: add historical data



else:
  # do daily data collection






  # then do dailiy refresh 



# # infotracer and sentiment: add historical data
# generate_infotracer_and_sentiment_table(start_date="2023-04-08",end_date="2023-04-16",ytb_end_date="2023-04-16",query_dict=query_dict,update_db=True)
# # network: add historical data
# generate_network_table(start_date="2023-04-08",end_date="2023-04-16",query_dict=query_dict,update_db=True)
# # wordcloud
# generate_wordcloud_table()
# '''


# """# Daily update"""

# print('##############################')
# print('Update starts')

# # helper: daily update
# print('helper table starts')
# today = datetime.now(pytz.timezone('America/Mexico_City')).date()
# tomorrow=datetime.now(pytz.timezone('America/Mexico_City')).date() + timedelta(days=1)
# generate_helper_table(start_date=tomorrow, end_date=tomorrow,query_dict=query_dict, update_db=True)
# print('helper table done')


# # infotracer and sentiment: daily update
# print('infotracer and sentiment table start')
# today = datetime.now(pytz.timezone('America/Mexico_City')).date().strftime('%Y-%m-%d')
# tomorrow=(datetime.now(pytz.timezone('America/Mexico_City')).date() + timedelta(days=1)).strftime('%Y-%m-%d')
# generate_infotracer_and_sentiment_table(start_date=today, end_date=tomorrow, ytb_end_date=tomorrow,query_dict=query_dict,update_db=True)
# print('infotracer and sentiment table done')

# # wordcloud: daily update
# print('wordcloud table starts')
# generate_wordcloud_table(query_dict=query_dict)
# print('wordcloud table done')

# # network: daily update
# print('network table starts')
# generate_network_table(start_date=today, end_date=tomorrow,query_dict=query_dict,update_db=True)
# print('network table done')

# print('Update ends')
# print('##############################')


# print('##############################')
# print('##############################')
# print('##############################')
# print('##############################')
# print('##############################')



# """# Refresh old data"""

# '''
# This code removes data from the date 48 hours ago and recollects it because the data 
# becomes more stable after that time period. This ensures that we are working 
# with the most reliable data possible.
# ''' 

# print('##############################')
# print('start refresh')
# two_days_ago=(datetime.now(pytz.timezone('America/Mexico_City')).date() - timedelta(days=2)).strftime('%Y-%m-%d')
# three_days_ago=(datetime.now(pytz.timezone('America/Mexico_City')).date() - timedelta(days=3)).strftime('%Y-%m-%d')

# # delete
# # data in db is mexico timezone
# infotracer_query="DELETE FROM infotracer WHERE datetime >= '"+three_days_ago+" 18:00:00' AND datetime <= '"+two_days_ago+" 18:00:00'"
# sentiment_query="DELETE FROM sentiment WHERE datetime >= '"+three_days_ago+" 18:00:00' AND datetime <= '"+two_days_ago+" 18:00:00'"

# mycursor.execute(infotracer_query)
# mycursor.execute(sentiment_query)

# mydb.commit()
# print('old data deleted')

# # recollect
# print('start recollect')
# generate_infotracer_and_sentiment_table(start_date=two_days_ago, end_date=two_days_ago, ytb_end_date=two_days_ago, query_dict=query_dict,update_db=True)
# print('recollect end')

# print('refresh ends')
# print('##############################')


