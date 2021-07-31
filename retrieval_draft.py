import os
import tweepy as tw
import pandas as pd

# Set access keys and tokens
consumer_key= ''
consumer_secret= ''
access_token= ''
access_token_secret= ''

auth = tw.OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_token_secret)
api = tw.API(auth, wait_on_rate_limit=True)

# Post a tweet from Python
api.update_status("Tweet through API, python test")

# Define the search term and the date_since date as variables
search_words = "$ETH"
date_since = "2021-03-27"

# Collect tweets
tweets = tw.Cursor(api.search,
              q=search_words,
              lang="en",
              since=date_since).items(5)
tweets


# Iterate and print tweets
for tweet in tweets:
    print(tweet.text)

# Collect a list of tweets
[tweet.text for tweet in tweets]

new_search = search_words + " -filter:retweets"
new_search

tweets = tw.Cursor(api.search,
                       q=new_search,
                       lang="en",
                       since=date_since).items(5)

text=[tweet.text for tweet in tweets]

tweets = tw.Cursor(api.search, 
                           q=new_search,
                           lang="en",
                           since=date_since).items(5)

users_locs = [[tweet.user.screen_name, tweet.user.location] for tweet in tweets]
users_locs

tweet_text = pd.DataFrame(data=users_locs, 
                    columns=['user', "location"])
tweet_text['text'] = text

# Import sentiment analysis library
import nltk
nltk.download('vader_lexicon')
from nltk.sentiment.vader import SentimentIntensityAnalyzer
analyzer = SentimentIntensityAnalyzer()

# Set empty array
eth_sentiments = []

# Populate the array with polarity scores
for tweet in tweet_text["text"]:
    print(tweet)
    try:
        sentiment = analyzer.polarity_scores(tweet)

        eth_sentiments.append({
            "Text": tweet,
            "Compound": sentiment["compound"],
            "Positive": sentiment["pos"],
            "Negative": sentiment["neg"],
            "Neutral": sentiment["neu"]
            
        })
        
    except AttributeError:
        pass
    
# Create DataFrame
eth_df = pd.DataFrame(eth_sentiments)

# Reorder DataFrame columns
cols = ["Compound", "Negative", "Neutral", "Positive", "Text"]
eth_df = eth_df[cols]

