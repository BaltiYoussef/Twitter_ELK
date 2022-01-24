#Libraries
import tweepy
from tweepy import OAuthHandler
from textwrap import TextWrapper
from elasticsearch import Elasticsearch
from textblob import TextBlob
from textblob_fr import PatternTagger, PatternAnalyzer
import sys
import re

#Connexion au Cloud
from elasticsearch import Elasticsearch, helpers
import configparser

config = configparser.ConfigParser()
config.read('example.ini')
es = Elasticsearch(cloud_id=config['ELASTIC']['cloud_id'],
                   http_auth=(config['ELASTIC']['user'], 
                              config['ELASTIC']['password'])
)


#identifiaction 
# Variables d'accès à l'API Twitter
access_token = ""
access_token_secret = ""
consumer_key = ""
consumer_secret = ""
BEARER_TOKEN = ""

# Authentication
auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_token_secret)
api = tweepy.API(auth)


def nlp_pipeline(text):

    text = text.lower()
    text = text.replace('\n', ' ').replace('\r', '')
    text = ' '.join(text.split())
    text = re.sub(r"[A-Za-z\.]*[0-9]+[A-Za-z%°\.]*", "", text)
    text = re.sub(r"(\s\-\s|-$)", "", text)
    text = re.sub(r"[,\!\?\%\(\)\/\"]", "", text)
    text = re.sub(r"\&\S*\s", "", text)
    text = re.sub(r"\&", "", text)
    text = re.sub(r"\+", "", text)
    text = re.sub(r"\#", "", text)
    text = re.sub(r"\$", "", text)
    text = re.sub(r"\£", "", text)
    text = re.sub(r"\%", "", text)
    text = re.sub(r"\:", "", text)
    text = re.sub(r"\@", "", text)
    text = re.sub(r"\-", "", text)

    return text


def analyse_sentiment(mot) :
    sentiment = "" 
    mot_clean = nlp_pipeline(mot)
    testimonial = TextBlob(mot_clean)
    lem=" ". join([w.lemmatize() for w in testimonial.words])
    res = TextBlob(lem,pos_tagger=PatternTagger(),analyzer=PatternAnalyzer()).sentiment
    if res[0] <0:
        sentiment = 'négatif'
    elif res[0] > 0 : 
        sentiment = 'positif'
    else: sentiment ='neutre'

    return ("sentiment : " + str(sentiment)), ("polarité : " + str(res[0])), ("subjectivité : " + str(res[1]))



class StreamApi(tweepy.StreamListener):
    status_wrapper = TextWrapper(width=60, initial_indent='    ', subsequent_indent='    ')
    
    def on_status(self, status):
    
        json_data = status._json
                

        #code pour recuperer le texte en entier      
        
        if 'retweeted_status' in status._json:
           if 'extended_tweet' in status._json['retweeted_status']:
              text = 'RT @'+status._json['retweeted_status']['user']['screen_name']+':'+status._json['retweeted_status']['extended_tweet']['full_text']
           else:
              text = 'RT @'+status._json['retweeted_status']['user']['screen_name']+':' +status._json['retweeted_status']['text']
        else:
           if 'extended_tweet' in status._json:
              text = status._json['extended_tweet']['full_text']
           else:
              text = status.text
        
        

        #Analyse de sentiment 
        
        retour_sentiment = analyse_sentiment(text)

        data={}
        #Création du dictionaire contenant les données
        data["id"] =  {"date": json_data["created_at"],
               "message": text,
               "polarity": retour_sentiment[1],
               "subjectivity": retour_sentiment[2],
               "sentiment": retour_sentiment[0]}

        es.indices.create(index = elkindex, ignore=400)
        
        es.index(index=elkindex,
                 doc_type='twitter',
                 body=data,
                 ignore=400)
    
terms = sys.argv[1:]
elkindex= sys.argv[1]
print(elkindex)
streamer = tweepy.Stream(auth = auth, listener=StreamApi(),timeout=30, tweet_mode='extended')
streamer.filter(None,terms,languages=["fr"])


