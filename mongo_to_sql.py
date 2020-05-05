#pip install mysql-connector

import mysql.connector
from mysql.connector import Error
import tweepy
import json
from dateutil import parser
import time
import os
import subprocess

try:
    con = mysql.connector.connect(host='localhost',database='twitterdb', user='root', password='7dc41992', charset='utf8')

    if con.is_connected():
        """
        Insert twitter data
        """
        cursor = con.cursor()
        # twitter, golf
        query = "INSERT INTO Test (username, created_at, tweet, retweet_count,place, location) VALUES (%s, %s, %s, %s, %s, %s)"
        cursor.execute(query, (username, created_at, tweet, retweet_count, place, location))
        con.commit()


except Error as e:
    print(e)

cursor.close()
con.close()