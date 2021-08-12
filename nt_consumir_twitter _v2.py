# Databricks notebook source
from pyspark import SparkFiles
import pyspark
from pyspark.sql.functions import *
from pyspark.sql.types import *
import pandas as pd
import demoji
import tweepy as tw
import json

# COMMAND ----------

# MAGIC %run /Repos/ruan.pomponet@gmail.com/git-bricks-case/db_connetion

# COMMAND ----------

selectQueryVLAM = "(select * from dbo.trusted_cons_vendas_linha_ano_mes) trusted_cons_vendas_marca_linha"
dfQueryVLAM = spark.read.jdbc(url=url,table=selectQueryVLAM,properties=properties)


# COMMAND ----------

dfQueryVLAM.createOrReplaceTempView("cons_vendas_linha_ano_mes_temp")

# COMMAND ----------

maxLinha = spark.sql('select linha  from cons_vendas_linha_ano_mes_temp where  ano = 2019 and mes = "12" and total_vendas =  (select max(total_vendas) from cons_vendas_linha_ano_mes_temp where ano = 2019 and mes = "12")').collect()[0][0]
print(maxLinha)

# COMMAND ----------

def remove_emoji(text):
    emoji_pattern = re.compile("["
                               u"\U0001F600-\U0001F64F"  # emoticons
                               u"\U0001F300-\U0001F5FF"  # symbols & pictographs
                               u"\U0001F680-\U0001F6FF"  # transport & map symbols
                               u"\U0001F1E0-\U0001F1FF"  # flags (iOS)
                               u"\U00002500-\U00002BEF"  # chinese char
                               u"\U00002702-\U000027B0"
                               u"\U00002702-\U000027B0"
                               u"\U000024C2-\U0001F251"
                               u"\U0001f926-\U0001f937"
                               u"\U00010000-\U0010ffff"
                               u"\u2640-\u2642"
                               u"\u2600-\u2B55"
                               u"\u200d"
                               u"\u23cf"
                               u"\u23e9"
                               u"\u231a"
                               u"\ufe0f"  # dingbats
                               u"\u3030"
                               "]+", flags=re.UNICODE)
    return emoji_pattern.sub(r'', text) 

# COMMAND ----------

consumer_key = '5ar7ezOePTVR77wle4r7qGanI',
consumer_secret = 'djestUw7gLnYcz1lqKonyAZs6Vf5qiFIwrJ0Kefj7X7Q38p3z5',
access_token = '1424897452138708992-XknWaNsEuplfSAhA1qFanj9W6L2Zgu',
access_token_secret = '9J8GiOATMiCGhTOiIfOO1KEuv5l6DFfJAAA4Im5mHs7WV'

# COMMAND ----------

keyWords = ['boticario']
lg = 'pt'
result = 'recent'

# COMMAND ----------

auth = tw.OAuthHandler(consumer_key,consumer_secret)
auth.set_access_token(access_token,access_token_secret)
api = tw.API(auth)



# COMMAND ----------

tweets = tw.Cursor(api.search,
                     q=keyWords,
                     ).items(50)

# COMMAND ----------

twPublic._json

# COMMAND ----------

for tweet in tweets:
    print(tweet.text)

# COMMAND ----------


  df = pd.DataFrame(columns=["created_at", "name", "tweet"]) 
 
  for tweet in ts.search_tweets_iterable(tso): 
    created_at = tweet['created_at']
    user = remove_emoji(tweet['user']['screen_name'])
    tweet = remove_emoji(tweet['text'])


    twitterDict = {'created_at': created_at,
                    'name': user,
                    'tweet' : tweet  
                  }

    df = df.append(twitterDict, ignore_index=True)
   


  

# COMMAND ----------

display(df)

# COMMAND ----------

df
df.count()

# COMMAND ----------

dfTweet = 
dfTweet.head()

# COMMAND ----------

dfTweets=[
              'CREATED_AT',
              'USER',
              'TWEET'
         ]

# COMMAND ----------

dfTT = (dfTweet
       .select(dfTweets)
       )
