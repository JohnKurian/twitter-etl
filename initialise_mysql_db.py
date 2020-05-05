import mysql.connector

mydb = mysql.connector.connect(
  host="127.0.0.1",
  user="root",
  passwd="7dc41992"
)

mycursor = mydb.cursor()

try:
    mycursor.execute("CREATE DATABASE twitterdb")
except:
    pass

mycursor.execute("CREATE TABLE twitterdb.test (id VARCHAR(255), user_name VARCHAR(255), text VARCHAR(255))")