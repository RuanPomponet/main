# Databricks notebook source
# MAGIC %md 
# MAGIC #### Import libs e conexões

# COMMAND ----------

from TwitterSearch import *
from pyspark import SparkFiles
import pyspark
from pyspark.sql.functions import *
from pyspark.sql.types import *
import pandas as pd
import demoji
import re

# COMMAND ----------

#Executa o notebook resposável pela conexão com azure sql server

# COMMAND ----------

# MAGIC %run /Repos/ruan.pomponet@gmail.com/git-bricks-case/db_connetion

# COMMAND ----------

# MAGIC %md
# MAGIC #### Funções principais

# COMMAND ----------

# MAGIC %md 
# MAGIC ##### Função que remove emoticons dos tweets

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

# MAGIC %md 
# MAGIC ##### Criação da função que constreio a data
# MAGIC * Transforma a data em formato por extenso(Str) para o formato (dd/MM/yyyy HH:mm:ss)  para que possa ser transofrma da em timestemp

# COMMAND ----------

def constroi_data(created_at = ''):
  
    #Faz o substring da data
    ano = created_at[26:]
    dia = created_at[8:10]
    mes = created_at[4:7]
    hora = created_at[11:19]
    
    #Retira os espaços entre os textos
    ano = ano.strip()
    dia = dia.strip()
    mes = mes.strip()
    hora = hora.strip()
    
    #Converte mês abreviado(en) para inteiro
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

# MAGIC %md 
# MAGIC #### Cria o dataframe e temp view com os dados da tabela das linhas de produtos mais vendidas para manipulação

# COMMAND ----------

#Função spark que busca via select os dados da tabela e cria o dataframe para manipulação dos dados
selectQueryVLAM = "(select * from dbo.trusted_cons_vendas_linha_ano_mes) trusted_cons_vendas_marca_linha"
dfQueryVLAM = spark.read.jdbc(url=url,table=selectQueryVLAM,properties=properties)


# COMMAND ----------

#View temporária para consumo dos dados de vendas e linhas
dfQueryVLAM.createOrReplaceTempView("cons_vendas_linha_ano_mes_temp")

# COMMAND ----------

#Query para para trazer a linha com mais venda em determinado período de tempo
maxLinha = spark.sql('select linha  from cons_vendas_linha_ano_mes_temp where  ano = 2019 and mes = "12" and total_vendas =  (select max(total_vendas) from cons_vendas_linha_ano_mes_temp where ano = 2019 and mes = "12")').collect()[0][0]

# COMMAND ----------

# MAGIC %md 
# MAGIC #### Configurações de autenticação e set de vaiáveis de busca

# COMMAND ----------

consumerKeyTw = dbutils.secrets.get(scope="key-vault-secrets",key="consumerKeyTw")
consumerSecretTw = dbutils.secrets.get(scope="key-vault-secrets",key="consumerSecretTw")
accessTokenTw = dbutils.secrets.get(scope="key-vault-secrets",key="accessTokenTw")
accessTokenSecretTw = dbutils.secrets.get(scope="key-vault-secrets",key="accessTokenSecretTw")

# COMMAND ----------

#Set de variáveis de autenticação
ts = TwitterSearch(
    consumer_key = consumerKeyTw,
    consumer_secret = consumerSecretTw,
    access_token = accessTokenTw,
    access_token_secret = accessTokenSecretTw
    
)

# COMMAND ----------

#Set de variáveis para construir a query de busca personalizada
tso = TwitterSearchOrder()
tso.set_keywords(['Boticario',maxLinha])
tso.set_language('pt')
tso.set_result_type('recent')


# COMMAND ----------

# MAGIC %md 
# MAGIC #### Lógica para consumo dos dados vindos da API do twitter utilizando metodos de busca da lib tweepy

# COMMAND ----------

#Cria e define colunas do dataframe que será gerado
df = pd.DataFrame(columns=["created_at", "user", "tweet"]) 

i = 0

#Interador de busca dos dados to twitter a partir da query tso
for tweet in ts.search_tweets_iterable(tso):
  
  created_at = tweet['created_at']
  user = remove_emoji(tweet['user']['screen_name']) #Chamada a func para remover emoji dos textos
  tweet = remove_emoji(tweet['text']) #Chamada a func para remover emoji dos textos
 
 #Chamada a func para construção data
  created_at = constroi_data(created_at)
  
  #Dict que receberá os dados para construção do dataframe
  twitterDict = { 
                  'created_at': created_at,
                  'user': user,
                  'tweet' : tweet  
                }
  

  df = df.append(twitterDict, ignore_index=True)
  
  
  #Limitador do número de tweet que serão consumidos
  i = i + 1  
  
  if i == 50:
    break;


  

# COMMAND ----------

# MAGIC %md 
# MAGIC #### Criação do spark dataframe, definição de schema e tratamendo de dados

# COMMAND ----------

#Converte do dataframe pandas para spark
df = spark.createDataFrame(df)

# COMMAND ----------

#Cria novo dataframe padronizando campos
dfTweets=[
              'CREATED_AT',
              'USER',
              'TWEET'
         ]

# COMMAND ----------

#Cria novo dataframe padronizado e converte created_at em formato timestemp
dfTT = (df
        .withColumn('created_at', to_timestamp(col('created_at'),'dd/MM/yyyy HH:mm:ss'))
        #Tratamendo de datas
       .select(dfTweets)
       )


# COMMAND ----------

# MAGIC %md 
# MAGIC #### Cria a tabela no Azure sql server

# COMMAND ----------

createTableTwitter = pyspark.sql.DataFrameWriter(dfTT)
createTableTwitter.jdbc(url=url, table="trusted_twitter_search", mode ="overwrite", properties = properties)

# COMMAND ----------

selectQuery = "(select * from dbo.trusted_twitter_search) trusted_twitter_search"
dfSelect = spark.read.jdbc(url=url,table=selectQuery,properties=properties)
