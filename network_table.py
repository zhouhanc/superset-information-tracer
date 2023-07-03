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



"""# ETL data for network"""


def generate_network_table(start_date, end_date, query_dict, config, update_db=True):
  

  # information tracer token

  your_token = config["infotracer_token"]

  for candidate, query in query_dict.items():
    ## extract data
    id_hash256 = informationtracer.trace(query=query, token=your_token, start_date=start_date, end_date=end_date, skip_result=True)
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


    nw_df['candidate_name']=candidate
	

  
    if update_db==True:

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

      ##### insert into mysql table source, target, weight, source_category, target_category
      nw_data=nw_df.apply(tuple, axis=1).tolist()
      query="insert into network (source, target, weight, source_cat, target_cat, source_datetime, target_datetime, candidate_name) Values(%s,%s,%s,%s,%s,%s,%s,%s);"
      mycursor.executemany(query,nw_data)

      mydb.commit()
      mydb.close()

  return

