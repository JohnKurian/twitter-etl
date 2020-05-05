import mysql.connector

mydb = mysql.connector.connect(
  host="127.0.0.1",
  user="root",
  passwd="7dc41992"
)

mycursor = mydb.cursor()

try:
    mycursor.execute("CREATE DATABASE twitterdb")
    mycursor.execute("USE twitterdb")
except:
    pass


#Dimension tables
mycursor.execute("""CREATE TABLE user 
(user_id double NOT NULL PRIMARY KEY, 
screen_name VARCHAR(255), 
followers_count VARCHAR(255))""")




mycursor.execute("""CREATE TABLE place 
(place_id double NOT NULL PRIMARY KEY, 
country VARCHAR(255), 
full_name VARCHAR(255))""")


#Fact table
mycursor.execute("""CREATE TABLE tweet 
(id double NOT NULL PRIMARY KEY, 
source VARCHAR(255), 
text VARCHAR(255),

user_id double NOT NULL,

FOREIGN KEY(user_id) REFERENCES user(user_id))
""")




print('created fact and dimension tables.')