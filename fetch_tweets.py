#download mysql
#https://dev.mysql.com/downloads/mysql/


# use screen to run this in the backgroud
# screen -S fetch_tweets -dm python3.7 fetch_tweets.py

# if you want to fetch tweet by object id
# db.tweets.find('5eb18f9827834a94ed99bceb')


import tweepy
import json
from dateutil import parser
import time
import os
import subprocess
import pymongo
from pymongo import MongoClient

consumer_key = "jjSz1RE4ftTNmqB1XuUTM22Fc"
consumer_secret = "5SNWrhQStzMp3UDwVY7YGuEofQ4QOBBP4rOo4hGnhKpdFQoVi9"
access_token = "68979886-IzdibLmYAx39y8PLNWA7kLPKl2rTDlLPCnf557I45"
access_token_secret = "tjVsF4mx4vS9JO0hPcS7b8qoP7oIZK1A8nX0aMwhkNEDG"


auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_token_secret)

myclient = MongoClient()
mydb = myclient["tweetbase"]
mycol = mydb["tweets"]
staging_col = mydb["tweets_staging"]


# import mysql.connector
# from mysql.connector import Error


# importing file which sets env variable
# subprocess.call("./settings.sh", shell=True)

# consumer_key = os.environ['CONSUMER_KEY']
# consumer_secret = os.environ['CONSUMER_SECRET']
# access_token = os.environ['ACCESS_TOKEN']
# access_token_secret = os.environ['ACCESS_TOKEN_SECRET']
# password = os.environ['PASSWORD']


# api = tweepy.API(auth)
# public_tweets = api.home_timeline()
# for tweet in public_tweets:
#     print(tweet.text)


def connect(username, created_at, tweet, retweet_count, place, location):
    """
    connect to MySQL database and insert twitter data
    """
    try:
        con = mysql.connector.connect(host='localhost',
                                      database='twitterdb', user='root', password=password, charset='utf8')

        if con.is_connected():
            """
            Insert twitter data
            """
            cursor = con.cursor()
            # twitter, golf
            query = "INSERT INTO Golf (username, created_at, tweet, retweet_count,place, location) VALUES (%s, %s, %s, %s, %s, %s)"
            cursor.execute(query, (username, created_at, tweet, retweet_count, place, location))
            con.commit()


    except Error as e:
        print(e)

    cursor.close()
    con.close()

    return


# Tweepy class to access Twitter API
class Streamlistener(tweepy.StreamListener):

    def on_connect(self):
        print("You are connected to the Twitter API")

    def on_error(self):
        if status_code != 200:
            print("error found")
            # returning false disconnects the stream
            return False

    """
    This method reads in tweet data as Json
    and extracts the data we want.
    """

    def on_data(self, data):

        try:
            raw_data = json.loads(data)

            if 'text' in raw_data:

                username = raw_data['user']['screen_name']
                created_at = parser.parse(raw_data['created_at'])
                tweet = raw_data['text']
                retweet_count = raw_data['retweet_count']

                if raw_data['place'] is not None:
                    place = raw_data['place']['country']
                    print(place)
                else:
                    place = None

                location = raw_data['user']['location']


            record = raw_data
            # record['processed'] = 0

            x = mycol.insert_one(record)
            y = staging_col.insert_one(record)

            print("Tweet colleted.")
        except:
            print('error')


if __name__ == '__main__':
    # # #Allow user input
    # track = []
    # while True:

    # 	input1  = input("what do you want to collect tweets on?: ")
    # 	track.append(input1)

    # 	input2 = input("Do you wish to enter another word? y/n ")
    # 	if input2 == 'n' or input2 == 'N':
    # 		break

    # print("You want to search for {}".format(track))
    # print("Initialising Connection to Twitter API....")
    # time.sleep(2)

    # authentification so we can access twitter
    auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)
    api = tweepy.API(auth, wait_on_rate_limit=True)

    # create instance of Streamlistener
    listener = Streamlistener(api=api)
    stream = tweepy.Stream(auth, listener=listener)

    track = ['#covid2019', '#covid19', '#coronavirus', '#gocorona', '#pandemic', '#coronaviren', '#coronavirusoutbreak', '#stayhome', '#covid-19', '#covidiot', '#covidiots', '#coronaviruspandemic']
    # track = ['nba', 'cavs', 'celtics', 'basketball']
    # choose what we want to filter by
    stream.filter(track=track, languages=['en'])

