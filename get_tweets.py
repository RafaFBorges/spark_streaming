import socket
import tweepy

HOST = 'localhost' 
PORT = 4243
TOKEN = 'AAAAAAAAAAAAAAAAAAAAAF25iAEAAAAAIpKgrnFQbf7i95HPDWCzmUWbp9w%3D1NYSeQv9jIuImrVkzeUcqURzvAMRDeMqVlKFN94n55gPh7cWH8'
keyword = 'bolsonaro'

s = socket.socket()
s.bind((HOST, PORT))
print(f'Abrindo conexão na porta : {PORT}')
s.listen(5)
conn, address = s.accept()

class GetTweets(tweepy.StreamingClient):
    def on_tweet(self, tweet):
        print(tweet.text)
        print('='*50)
        conn.send(tweet.text.encode('utf-8', 'ignore'))

tweets = GetTweets(TOKEN)
tweets.add_rules(tweepy.StreamRule(keyword))
tweets.filter()

conn.close()
