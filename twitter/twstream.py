import sys
import tweepy
import json
from threading import Thread
from Queue import Queue

#Get those from dev.twitter.com, you must create an application to get them
CONSUMER_KEY = 'ZkpEmsYvjrRmQK0iODTzw'
CONSUMER_SECRET = 'myWcVjgaQkLpZab0fWj0KVaiayZzbnyCHp3RzS8HnI'
ACCESS_TOKEN = '67731745-zPkJ7hwCZXaLgb7nDQECjX6KoWjHoIC16G5a54jQZ'
ACCESS_TOKEN_SECRET = 'HCSnMexLYqeypbWox2p89nFOgldDx6rf5Aj6F4NKA'


#CONSUMER_KEY = 'CONSUMER_KEY'
#CONSUMER_SECRET = 'CONSUMER_SECRET'
#ACCESS_TOKEN = 'ACCESS TOKEN'
#ACCESS_TOKEN_SECRET = 'ACCESS TOKEN SECRET'

#This is the object used by tweepy to notify whenever
#a tweet is received
class StreamingListener(tweepy.StreamListener):
    def __init__(self, queue):
        super(StreamingListener, self).__init__()

        #We get the reference to the queue where we need
        #to put all the data we receive from twitter
        self.queue = queue
    
    def on_data(self, data):
        if 'in_reply_to_status_id' in data:
            status = data
            if self.on_status(status) is False:
                return False
        elif 'delete' in data:
            delete = json.loads(data)['delete']['status']
            if self.on_delete(delete['id'], delete['user_id']) is False:
                return False
        elif 'limit' in data:
            if self.on_limit(json.loads(data)['limit']['track']) is False:
                return False        
         
    def on_status(self, status):
        #Whenever we receive a new tweet we just put it inside the queue
        try:
            self.queue.put(status ,block=False)
            #self.queue.put(status2dict(status) ,block=False)
        except Exception as e:
            print self.queue.qsize()
            pass

    def on_error(self, status_code):
        print 'Encountered error with status code:', status_code
        return False

def stream_data(filters):
    #Queue where we are going to store the tweets retrieved from twitter
    queue = Queue()

    listener = StreamingListener(queue)

    #The authentication data we need to access twitter 
    auth = tweepy.OAuthHandler(CONSUMER_KEY, CONSUMER_SECRET)
    auth.set_access_token(ACCESS_TOKEN, ACCESS_TOKEN_SECRET)

    #Create the stream fetching object with the provided authentication dat
    #and the StreamListener that stores inside the queue what it receives
    stream = tweepy.streaming.Stream(auth, listener)

    #To avoid blocking our function in fetching data from twitter
    #we perform the actual read of data from a separate Thread
    t = Thread(target=lambda: stream.filter(track=filters))
    t.daemon = True
    t.start()

    #This is a generator that permits to consume the tweets
    #available inside the queue. So we can expose
    #the tweets as a "list" that never ends.
    def _generator():
        while True:
            yield queue.get()

    #Return the generator so that who called stream_data
    #can iterate over it to fetch the tweets.
    return _generator()

#Fetch all the tweets for 'basketball' and print them

if __name__ == '__main__':
    filters = sys.argv[1:]
    if filters:
        ct = 0
        print "Twitter streaming for ", filters
        for tw in stream_data(filters):
            print tw

    else:
        print "Please provide some filters"
