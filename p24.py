from tweepy.streaming import StreamListener
from tweepy import OAuthHandler 
from tweepy import Stream 
from kafka import SimpleProducer, KafkaClient 
import json 
rec = {}
access_token = "125884122-MPAWhVMlBT2gXRx1RAtE0MqWS5Y13lNv1dcX4hSh"
access_token_secret = "dB5witc68PVjc779i3emteUdm9BuhdhDHhSNSgGHiXBP1"
consumer_key = "t5AiNxL484WXBGgUHPJtq5MR8" 
consumer_secret = "KVz3ru0OAKeD7js8LT6HcrHt3jF6r1pKEuGC2aTQAElLoehEZg" 
class StdOutListener(StreamListener):
    def on_data(self, data):
        all_data = json.loads(data)
		# collect all desired data fields
        if 'text' in all_data:
          tweet = all_data["text"]
          created_at = all_data["created_at"]
          retweeted = all_data["retweeted"]
          username = all_data["user"]["screen_name"]
          user_tz = all_data["user"]["time_zone"]
          user_location = all_data["user"]["location"]
          user_coordinates = all_data["coordinates"]
		  
	  # if coordinates are not present store blank value otherwise get the coordinates.coordinates value
          if user_coordinates is None:
            final_coordinates = user_coordinates
          else:
            final_coordinates = str(all_data["coordinates"]["coordinates"])
          
	  if user_location != None:
		print 'Created at:' + created_at +' username: ' + username + ' user_location: ' + user_location 
                rec = {'created_at': created_at.encode('utf-8'), 'username':username.encode('utf	-8'),'location':user_location.encode('utf-8')}
	        producer.send_messages("rusia",json.dumps(rec,separators=(',',':')))
	return True
		
    def on_error(self, status):
        print (status) 
kafka = KafkaClient("localhost:9092") 
producer = SimpleProducer(kafka)
l = StdOutListener() 
auth = OAuthHandler(consumer_key, consumer_secret) 
auth.set_access_token(access_token, access_token_secret) 
stream = Stream(auth, l)
stream.filter(track="rusia")

