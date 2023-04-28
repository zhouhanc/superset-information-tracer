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

# import functions from other files
from helper import generate_helper_table
from infotracer_and_sentiment import generate_infotracer_and_sentiment_table
from wordcloud_table import generate_wordcloud_table
from network_table import generate_network_table




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


query_dict={'Manolo Jiménez Salinas':'"Manolo Jiménez Salinas" OR manolojim OR manolojimenezs OR Manolo.Jimenez.Salinas',
      'Armando Guadiana Tijerina':'"Armando Guadiana Tijerina" OR aguadiana OR armandoguadianatijerina OR ArmandoGuadianaTijerina',
'Ricardo Mejia Berdeja':'"Ricardo Mejia Berdeja" OR RicardoMeb OR ricardomeb OR RicardoMejiaMx',
    'Lenin Perez Rivera':'"Lenin Perez Rivera" OR leninperezr OR leninperezr OR leninperezr'
}


"""# Historical"""
'''
# helper: add historical data
generate_helper_table(start_date="2023-04-09",end_date="2023-04-16",query_dict=query_dict, update_db=True)

# infotracer and sentiment: add historical data
generate_infotracer_and_sentiment_table(start_date="2023-04-08",end_date="2023-04-16",ytb_end_date="2023-04-16",query_dict=query_dict,update_db=True)
# network: add historical data
generate_network_table(start_date="2023-04-08",end_date="2023-04-16",query_dict=query_dict,update_db=True)
# wordcloud
generate_wordcloud_table()
'''


"""# Daily update"""

print('##############################')
print('Update starts')

# helper: daily update
print('helper table starts')
today = datetime.now(pytz.timezone('America/Mexico_City')).date()
tomorrow=datetime.now(pytz.timezone('America/Mexico_City')).date() + timedelta(days=1)
generate_helper_table(start_date=tomorrow, end_date=tomorrow,query_dict=query_dict, update_db=True)
print('helper table done')


# infotracer and sentiment: daily update
print('infotracer and sentiment table start')
today = datetime.now(pytz.timezone('America/Mexico_City')).date().strftime('%Y-%m-%d')
tomorrow=(datetime.now(pytz.timezone('America/Mexico_City')).date() + timedelta(days=1)).strftime('%Y-%m-%d')
generate_infotracer_and_sentiment_table(start_date=today, end_date=tomorrow, ytb_end_date=tomorrow,query_dict=query_dict,update_db=True)
print('infotracer and sentiment table done')

# wordcloud: daily update
print('wordcloud table starts')
generate_wordcloud_table()
print('wordcloud table done')

# network: daily update
print('network table starts')
generate_network_table(start_date=today, end_date=tomorrow,query_dict=query_dict,update_db=True)
print('network table done')

print('Update ends')
print('##############################')


print('##############################')
print('##############################')
print('##############################')
print('##############################')
print('##############################')



"""# Refresh old data"""

'''
This code removes data from the date 48 hours ago and recollects it because the data 
becomes more stable after that time period. This ensures that we are working 
with the most reliable data possible.
''' 

print('##############################')
print('start refresh')
two_days_ago=(datetime.now(pytz.timezone('America/Mexico_City')).date() - timedelta(days=2)).strftime('%Y-%m-%d')
three_days_ago=(datetime.now(pytz.timezone('America/Mexico_City')).date() - timedelta(days=3)).strftime('%Y-%m-%d')

# delete
# data in db is mexico timezone
infotracer_query="DELETE FROM infotracer WHERE datetime >= '"+three_days_ago+" 18:00:00' AND datetime <= '"+two_days_ago+" 18:00:00'"
sentiment_query="DELETE FROM sentiment WHERE datetime >= '"+three_days_ago+" 18:00:00' AND datetime <= '"+two_days_ago+" 18:00:00'"

mycursor.execute(infotracer_query)
mycursor.execute(sentiment_query)

mydb.commit()
print('old data deleted')

# recollect
print('start recollect')
generate_infotracer_and_sentiment_table(start_date=two_days_ago, end_date=two_days_ago, ytb_end_date=two_days_ago, query_dict=query_dict,update_db=True)
print('recollect end')

print('refresh ends')
print('##############################')


