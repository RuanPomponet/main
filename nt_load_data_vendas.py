# Databricks notebook source
# MAGIC %md 
# MAGIC #### Import libs e conexões

# COMMAND ----------

from pyspark import SparkFiles
import pyspark
from pyspark.sql.functions import *
from pyspark.sql.types import *
import pandas as pd
import os
import sys
from glob import glob
import xlsxwriter

# COMMAND ----------

#Executa o notebook resposável pela conexão com azure sql server

# COMMAND ----------

# MAGIC %run /Repos/ruan.pomponet@gmail.com/git-bricks-case/db_connetion

# COMMAND ----------

# MAGIC %md 
# MAGIC #### set de variáveis

# COMMAND ----------

ingestion = '/dbfs/mnt/ingestion/excel/'
files = os.listdir(ingestion)

# COMMAND ----------

# MAGIC %md 
# MAGIC #### Import e consolidação dos arquivos xlsx em um dataframe

# COMMAND ----------

#Cria o datafreme que receberá os dados dos arquivos xlsx
dfApp = pd.DataFrame() 

for file in files:
  df = pd.read_excel(raw + file ,engine='openpyxl')
  dfApp = dfApp.append(df)

# COMMAND ----------

# MAGIC %md 
# MAGIC #### Criação do spark dataframe, definição de schema e tratamendo de dados

# COMMAND ----------

#Converte do dataframe pandas para spark
df = spark.createDataFrame(dfApp) 

# COMMAND ----------

#Cria novo dataframe padronizando campos
dfVendas=[
              'ID_MARCA',
              'MARCA',
              'ID_LINHA',
              'LINHA',
              'DATA_VENDA',
              'QTD_VENDA'
         ]

# COMMAND ----------

#Cria novo dataframe padronizado e insere data types
creatTableDf = (df
               .withColumn('DATA_VENDA',to_date(col('DATA_VENDA'),'dd/MM/yyyy'))
               #tratamento de datas
               .withColumn('ID_MARCA', col('ID_MARCA').cast('int'))
               .withColumn('ID_LINHA', col('ID_LINHA').cast('int'))
               .withColumn('QTD_VENDA', col('QTD_VENDA').cast('int'))
               .select(dfVendas)
               )

# COMMAND ----------

# MAGIC %md 
# MAGIC #### Cria a tabela no Azure sql server

# COMMAND ----------

createTableVendas = pyspark.sql.DataFrameWriter(creatTableDf)
createTableVendas.jdbc(url=url, table="raw_vendas", mode ="overwrite", properties = properties)
