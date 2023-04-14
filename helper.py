"""# Import"""

import os

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

import itertools

import re
import string



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

    # Insert to db
    helper_data = helper_df.apply(tuple, axis=1).tolist()
    # change order of column to fit df
    query="insert into helper (datetime,platform,label, candidate_name) Values(%s,%s,%s,%s);" 
    mycursor.executemany(query,helper_data)
    mydb.commit()

  return
