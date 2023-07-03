# -*- coding: utf-8 -*-

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

# from pysentimiento.preprocessing import preprocess_tweet
# from pysentimiento import create_analyzer


import itertools

import re
import string
# from nltk.stem.snowball import SnowballStemmer
import nltk
# from gensim import corpora, models
from wordcloud import WordCloud

# import functions from other files
from helper import generate_helper_table
from infotracer_and_sentiment import generate_infotracer_table
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
  mycursor.execute("CREATE DATABASE {}".format(database_name))
  

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
 


else: 
  print(database_name,"exists")

  if config["delete_all_table_and_restart"] == True:
    # delete table
    mycursor.execute("drop table if exists infotracer")
    mycursor.execute("drop table if exists sentiment")
    mycursor.execute("drop table if exists wordcloud")
    mycursor.execute("drop table if exists network")
    mycursor.execute("drop table if exists helper")
    mydb.commit()

    # recreate table and schema
    mycursor.execute("create table infotracer (candidate_name varchar(255), text MediumTEXT, username varchar(255), num_interaction int, datetime timestamp, platform varchar(255) )")
    mycursor.execute("create table sentiment (text MediumTEXT, processed_text MediumTEXT, positive float, neutral float, negative float, label varchar(255),candidate_name varchar(255), platform varchar(255), username varchar(255), num_interaction int, datetime timestamp )")
    mycursor.execute("create table wordcloud (word varchar(255), weight float, candidate_name varchar(255),platform varchar(255))")
    mycursor.execute("create table network (source varchar(255), target varchar(255), weight float, source_cat varchar(255), target_cat varchar(255), source_datetime timestamp, target_datetime timestamp, candidate_name varchar(255))")
    mycursor.execute("create table helper (datetime timestamp,candidate_name varchar(255),label varchar(255),platform varchar(255))")
  





# start data collection
print("the data collection is for the following candidates:", config["query_dict"].keys())


# use config to choose: historical OR daily + refresh
# each block should be surrounded by try except

if config["date"]["predefined_period"]==True:
  # do historical data collection
  print('##############################')
  print("historical data collection")


  # helper: add historical data
  if config["generate_helper_table"]["run"]==True:
    try:
      print('helper table starts')
      generate_helper_table(start_date = config["date"]["start_date"], 
                            end_date = config["date"]["end_date"], 
                            query_dict = config["query_dict"], 
                            config=config,
                            update_db=config["generate_helper_table"]["update_db"]
                            )
      print('helper table ends')                      
    except Exception as e:
      print(f"An error occurred for helper table when updating historical data: {e}")
  

  # infotracer: add historical data
  # this should generally be false
  if config["generate_infotracer_table"]["run"]==True:
    try:
      print('infotracer table starts')
      generate_infotracer_table(start_date = config["date"]["start_date"], 
                                end_date = config["date"]["end_date"], 
                                query_dict = config["query_dict"], 
                                config=config,
                                update_db = config["generate_infotracer_table"]["update_db"]
                                )
      print('infotracer table ends')
    except Exception as e:
      print(f"An error occurred for infotracer table when updating historical data: {e}")


  # infotracer and sentiment: add historical data
  # this collect and update two table
  if config["generate_infotracer_and_sentiment_table"]["run"]==True:
    try:
      print('infotracer and sentiment table start')
      generate_infotracer_and_sentiment_table(start_date = config["date"]["start_date"],
                                              end_date = config["date"]["end_date"],  
                                              ytb_end_date = config["date"]["end_date"],  
                                              query_dict = config["query_dict"], 
                                              config=config,
                                              update_db = config["generate_infotracer_and_sentiment_table"]["update_db"]
                                              )
      print('infotracer and sentiment table end')
    except Exception as e:
      print(f"An error occurred for infotracer and sentiment table when updating historical data: {e}")


  # network: add historical data
  if config["generate_network_table"]["run"]==True:
    try:
      print('network table starts')
      generate_network_table(start_date = config["date"]["start_date"],
                             end_date = config["date"]["end_date"],
                             query_dict = config["query_dict"], 
                             config=config,
                             update_db = config["generate_network_table"]["update_db"]
                             )
      print('network table ends')
    except Exception as e:
      print(f"An error occurred for network table when updating historical data: {e}")


  # wordcloud: generate wordcloud using historical data
  if config["generate_wordcloud_table"]["run"]==True:
    try:
      print('wordcloud table starts')
      generate_network_table(query_dict = config["query_dict"], 
                             config=config,
                             update_db = config["generate_wordcloud_table"]["update_db"]
                             )
      print('wordcloud table ends')
    except Exception as e:
      print(f"An error occurred for wordcloud table when updating historical data: {e}")
    
    print("historical data collection ends")
    print('##############################')



else:
  # do daily data collection
  print('##############################')
  print('daily update starts')

  # helper: daily collect 
  if config["generate_helper_table"]["run"]==True:
    try:
      print('helper table starts')

      today = datetime.now(pytz.UTC).date()
      tomorrow = datetime.now(pytz.UTC).date() + timedelta(days=1)

      generate_helper_table(start_date = today, 
                            end_date = tomorrow, 
                            query_dict = config["query_dict"], 
                            config=config,
                            update_db=config["generate_helper_table"]["update_db"]
                            )
      print('helper table ends')                      
    except Exception as e:
      print(f"An error occurred for helper table when updating historical data: {e}")
  

  # infotracer: daily collect
  # this should generally be false
  if config["generate_infotracer_table"]["run"]==True:
    try:
      print('infotracer table starts')

      today = datetime.now(pytz.UTC).date().strftime('%Y-%m-%d')
      tomorrow = (datetime.now(pytz.UTC).date() + timedelta(days=1)).strftime('%Y-%m-%d')

      generate_infotracer_table(start_date = today, 
                                end_date = tomorrow, 
                                query_dict = config["query_dict"], 
                                config=config,
                                update_db = config["generate_infotracer_table"]["update_db"]
                                )
      print('infotracer table ends')
    except Exception as e:
      print(f"An error occurred for infotracer table when updating historical data: {e}")


  # infotracer and sentiment: daily collect
  # this collect and update two table
  if config["generate_infotracer_and_sentiment_table"]["run"]==True:
    try:
      print('infotracer and sentiment table start')

      today = datetime.now(pytz.UTC).date().strftime('%Y-%m-%d')
      tomorrow = (datetime.now(pytz.UTC).date() + timedelta(days=1)).strftime('%Y-%m-%d')

      generate_infotracer_and_sentiment_table(start_date = today,
                                              end_date = tomorrow,  
                                              ytb_end_date = tomorrow,  
                                              query_dict = config["query_dict"], 
                                              config=config,
                                              update_db = config["generate_infotracer_and_sentiment_table"]["update_db"]
                                              )
      print('infotracer and sentiment table end')
    except Exception as e:
      print(f"An error occurred for infotracer and sentiment table when updating historical data: {e}")


  # network: daily collect
  if config["generate_network_table"]["run"]==True:
    try:
      print('network table starts')

      today = datetime.now(pytz.UTC).date().strftime('%Y-%m-%d')
      tomorrow = (datetime.now(pytz.UTC).date() + timedelta(days=1)).strftime('%Y-%m-%d')

      generate_network_table(start_date = today,
                             end_date = tomorrow,  
                             query_dict = config["query_dict"], 
                             config=config,
                             update_db = config["generate_network_table"]["update_db"]
                             )
      print('network table ends')
    except Exception as e:
      print(f"An error occurred for network table when updating historical data: {e}")


  # wordcloud: generate wordcloud daily (use last n days from db, irrelavant to current date)
  if config["generate_wordcloud_table"]["run"]==True:
    try:
      print('wordcloud table starts')
      generate_network_table(query_dict = config["query_dict"], 
                             config=config,
                             update_db = config["generate_wordcloud_table"]["update_db"]
                             )
      print('wordcloud table ends')
    except Exception as e:
      print(f"An error occurred for wordcloud table when updating historical data: {e}")
    

    print("daily data collection ends")
    print('##############################')



  # do daily refresh
  # This module removes data from the date 48 hours ago and recollects it because the data 
  # becomes more stable after that time period. This ensures that we are working 
  # with the most reliable data possible.
  print('##############################')
  print('##############################')
  print('##############################')

  if config["refresh"]["run"]==True:
    print('daily refresh starts')

    # delete old data from db
    print("start deleting old data")

    two_days_ago = (datetime.now(pytz.UTC).date() - timedelta(days=2)).strftime('%Y-%m-%d')

    # Create the SQL query to delete records during the day two days ago
    sentiment_query = "DELETE FROM sentiment WHERE DATE(datetime) = '{}'".format(two_days_ago)
    infotracer_query = "DELETE FROM infotracer WHERE DATE(datetime) = '{}'".format(two_days_ago)

    mycursor.execute(infotracer_query)
    mycursor.execute(sentiment_query)

    mydb.commit()

    print('old data deleted')


    # recollect to refresh
    
    print('start recollect')

  
    try:
      print('infotracer and sentiment table start')

      generate_infotracer_and_sentiment_table(start_date = two_days_ago, 
                                              end_date = two_days_ago,
                                              ytb_end_date = two_days_ago,  
                                              query_dict = config["query_dict"], 
                                              config=config,
                                              update_db = config["refresh"]["update_db"]
                                              )
      print('infotracer and sentiment table end')
    except Exception as e:
      print(f"An error occurred for infotracer and sentiment table when updating historical data: {e}")


    print('recollect end')

    print('refresh ends')
    print('##############################')


