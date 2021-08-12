# Databricks notebook source
from pyspark import SparkFiles
import pyspark
from pyspark.sql.functions import *
from pyspark.sql.types import *
import pandas as pd
import os
from os import *
from os.path import *
import sys
from glob import glob
import xlsxwriter

# COMMAND ----------

raw = '/mnt/landing/csv'

# COMMAND ----------

files = glob('/dbfs/mnt/ingestion/excel/*.xlsx')


# COMMAND ----------

dfAppen = pd.DataFrame() 

for file in files:
  df = pd.read_excel(file ,engine='openpyxl')
  dfAppen = dfAppen.append(df)


# COMMAND ----------

# MAGIC %run /Users/ruan.pomponet@gmail.com/db_connetion

# COMMAND ----------

df = spark.createDataFrame(dfAppen) 

# COMMAND ----------

dfVendas=[
              'ID_MARCA',
              'MARCA',
              'ID_LINHA',
              'LINHA',
              'DATA_VENDA',
              'QTD_VENDA'
         ]

# COMMAND ----------

creatTableDf = (df
               .withColumn('DATA_VENDA',to_date(col('DATA_VENDA'),'dd/MM/yyyy'))
               #tratamento de datas
               .withColumn('ID_MARCA', col('ID_MARCA').cast('int'))
               .withColumn('ID_LINHA', col('ID_LINHA').cast('int'))
               .withColumn('QTD_VENDA', col('QTD_VENDA').cast('int'))
               .select(dfVendas)
               )

# COMMAND ----------

createTableVendas = pyspark.sql.DataFrameWriter(creatTableDf)
createTableVendas.jdbc(url=url, table="raw_vendas", mode ="overwrite", properties = properties)

# COMMAND ----------


