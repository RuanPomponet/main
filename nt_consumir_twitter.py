# Databricks notebook source
from TwitterSearch import *
from pyspark import SparkFiles
import pyspark
from pyspark.sql.functions import *
from pyspark.sql.types import *
import pandas as pd
import demoji
import re

# COMMAND ----------

# MAGIC %run /Repos/ruan.pomponet@gmail.com/git-bricks-case/db_connetion

# COMMAND ----------

def constroi_data(created_at = ''):
    ano = created_at[26:]
    dia = created_at[8:10]
    mes = created_at[4:7]
    hora = created_at[11:19]
    
    #Retira espaço dos textos
    ano = ano.strip()
    dia = dia.strip()
    mes = mes.strip()
    hora = hora.strip()
    
    
    if mes == 'Jan':
        mes = '01'
    elif mes == 'Feb':
        mes = '02'
    elif mes == 'Mar':
        mes = '03'
    elif mes == 'Apr':
        mes = '04'
    elif mes == 'May':
        mes = '05'
    elif mes == 'June':
        mes = '06'
    elif mes == 'Jul':
        mes = '07'
    elif mes == 'Aug':
        mes = '08'
    elif mes == 'Sept':
        mes = '09'
    elif mes == 'Oct':
        mes = '10'
    elif mes == 'Nov':
        mes = '11'
    else: mes = '12'

    data_corrigida = f'{dia}/{mes}/{ano} {hora}'
    return data_corrigida

# COMMAND ----------

selectQueryVLAM = "(select * from dbo.trusted_cons_vendas_linha_ano_mes) trusted_cons_vendas_marca_linha"
dfQueryVLAM = spark.read.jdbc(url=url,table=selectQueryVLAM,properties=properties)


# COMMAND ----------

dfQueryVLAM.createOrReplaceTempView("cons_vendas_linha_ano_mes_temp")

# COMMAND ----------

maxLinha = spark.sql('select linha  from cons_vendas_linha_ano_mes_temp where  ano = 2019 and mes = "12" and total_vendas =  (select max(total_vendas) from cons_vendas_linha_ano_mes_temp where ano = 2019 and mes = "12")').collect()[0][0]

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

ts = TwitterSearch(
    consumer_key = 'VgRNbxUhiEaWXkziFppEhAK91',
    consumer_secret = 'XNSWbinPA36vMmDRYBzpofm4Be3ajABCtylBWRPZburIaHU7Am',
    access_token = '1424897452138708992-kNO40VyaMWQGxyCXaBInd0p8haVqv8',
    access_token_secret = 'v7LtpifcGszinLQupxTLNcX3HWijwRvpVvzTJeszXTF3K'
    
)

# COMMAND ----------

tso = TwitterSearchOrder()
tso.set_keywords(['Boticario'])
tso.set_language('pt')
tso.set_result_type('recent')


# COMMAND ----------


  df = pd.DataFrame(columns=["created_at", "user", "tweet"]) 
  i = 0
  for tweet in ts.search_tweets_iterable(tso): 
    created_at = tweet['created_at']
    user = remove_emoji(tweet['user']['screen_name'])
    tweet = remove_emoji(tweet['text'])
    
    created_at = constroi_data(created_at)

    twitterDict = {'created_at': created_at,
                    'user': user,
                    'tweet' : tweet  
                  }
    
    df = df.append(twitterDict, ignore_index=True)
    
    i = i + 1    
    if i == 50:
      break;


  

# COMMAND ----------

df.head()

# COMMAND ----------

df = spark.createDataFrame(df)


# COMMAND ----------

df.show()

# COMMAND ----------

dfTweets=[
              'CREATED_AT',
              'USER',
              'TWEET'
         ]

# COMMAND ----------

dfTT = (df
        .withColumn('created_at', to_timestamp(col('created_at'),'dd/MM/yyyy HH:mm:ss'))
       .select(dfTweets)
       )


# COMMAND ----------

createTableTwitter = pyspark.sql.DataFrameWriter(dfTT)
createTableTwitter.jdbc(url=url, table="trusted_twitter_search", mode ="overwrite", properties = properties)

# COMMAND ----------

selectQuery = "(select * from dbo.trusted_twitter_search) trusted_twitter_search"
dfSelect = spark.read.jdbc(url=url,table=selectQuery,properties=properties)
