from kafka import KafkaConsumer
from pymongo import MongoClient
import json



try:
   client = MongoClient('localhost',27017)
   db = client.Twittersuse 
   print("Connected successfully!!!")
except:  
   print("Could not connect to MongoDB")
	
consumer = KafkaConsumer('mexico') #topic name
for msg in consumer:
    record = json.loads(msg.value)
    username = record['username']
    location = record['location']

    try:
       twitter_rec = {'name' :username,'location':location}
       rec_id1 = db.terrorism.insert_one(twitter_rec)
       print("Data inserted with record ids",rec_id1)
    except:
       print("Could not insertInMongo")

