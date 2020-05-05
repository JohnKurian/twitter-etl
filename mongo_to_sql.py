#Download driver from https://dev.mysql.com/downloads/connector/python/

#pip install mysql-connector-python

#start daemon
#sudo /usr/local/mysql/bin/mysqld_safe

#create database twitterdb
#

#to enter into the terminal
#/usr/local/mysql/bin/mysql -u root -p
#/usr/local/mysql/bin/mysqld --user=_mysql --basedir=/usr/local/mysql --datadir=/usr/local/mysql/data --plugin-dir=/usr/local/mysql/lib/plugin --log-error=/usr/local/mysql/data/mysqld.local.err --pid-file=/usr/local/mysql/data/mysqld.local.pid --keyring-file-data=/usr/local/mysql/keyring/keyring --early-plugin-load=keyring_file=keyring_file.so


import mysql.connector
from mysql.connector import Error
import tweepy
import json
from dateutil import parser
import time
import os
import subprocess
from pymongo import MongoClient

myclient = MongoClient()
mydb = myclient["tweetbase"]
staging_col = mydb["tweets_staging"]


cursor = staging_col.find({})





for document in cursor:
    try:

        con = mysql.connector.connect(host='127.0.0.1',
                                      database='twitterdb',
                                      user='root',
                                      password='7dc41992',
                                      charset='utf8',
                                      auth_plugin='mysql_native_password')

        if con.is_connected():
            """
            Insert twitter data
            """

            # twitter, golf
            # query = "INSERT INTO Test (username, created_at, tweet, retweet_count,place, location) VALUES (%s, %s, %s, %s, %s, %s)"
            # cursor.execute(query, (username, created_at, tweet, retweet_count, place, location))
            cursor = con.cursor()
            query = "INSERT INTO user (user_id, screen_name, followers_count) VALUES (%s, %s, %s)"
            print('user id:', document['user']['id'])
            cursor.execute(query, (
            document['user']['id'], document['user']['screen_name'], document['user']['followers_count']))
            con.commit()
            cursor.close()


            cursor = con.cursor()
            query = "INSERT INTO tweet (id, source, text, user_id) VALUES (%s, %s, %s, %s)"
            cursor.execute(query, (document['id'], document['source'], document['text'], document['user']['id']))
            con.commit()
            cursor.close()


            # query = "INSERT INTO place (place_id, country, full_name) VALUES (%s, %s, %s)"
            # cursor.execute(query, (document['place']['place']['id'], document['user']['name'], document['text']))


        con.close()


    except Error as e:
        print(e)

