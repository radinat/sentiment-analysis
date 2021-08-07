#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import tweepy as tw
import pandas as pd
import time

# Set access keys and tokens
consumer_key= ''
consumer_secret= ''
access_token= ''
access_token_secret= ''

auth = tw.OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_token_secret)

api = tw.API(auth, wait_on_rate_limit=True, wait_on_rate_limit_notify=True)


# Define the search term and the date_since date as variables
search_words = "$ETH" + " -filter:retweets"
date_since = "2021-04-01"

#Collect
# for tweet in tweets:
#     try:
#         text.append([tweet.text, tweet.user.screen_name, tweet.user.location, tweet.user.followers_count, tweet.user.statuses_count, tweet.user.verified, tweet.created_at, tweet.favorite_count, tweet.retweet_count])
#     except tw.TweepError:
#         time.sleep(60 * 15)
#         print("time error")
#         continue
#     except StopIteration:
#         print("stop")
#         break

tweets = tw.Cursor(api.search, 
                           q=search_words,
                           lang="en",
                           since=date_since).items(10000)

text=[[tweet.text, tweet.user.screen_name, tweet.user.location, tweet.user.followers_count, tweet.user.statuses_count, tweet.user.verified, tweet.created_at, tweet.favorite_count, tweet.retweet_count] 
      for tweet in tweets]
tweets_df = pd.DataFrame(text, 
                       columns=["text","username", "location", "followers", "tweets", "verified", "created_at","favs","retweets"])

print(tweets_df.iloc[-1, :])

tweets_df.to_csv('tweets_eth_06-08_07-08.csv', index=True)


