# Databricks notebook source
import pytz
from pyspark import SparkFiles
import pyspark
from pyspark.sql.functions import *
from pyspark.sql.types import *
import pandas as pd
import glob
import os
import pyodbc
import sys
sp_timezone = pytz.timezone('America/Sao_Paulo')

# COMMAND ----------

# MAGIC %run /Users/ruan.pomponet@gmail.com/db_connetion

# COMMAND ----------

selectQuery = "(select * from dbo.raw_vendas ) raw_vendas"
dfSelect = spark.read.jdbc(url=url,table=selectQuery,properties=properties)

# COMMAND ----------

dfLinhaAM = [
              'LINHA',
              'ANO',
              'MES',
              'TOTAL_VENDAS'
            ]

# COMMAND ----------

dfLinhaAnoMes = ( dfSelect
                  .withColumn('ANO', year('DATA_VENDA'))
                  .withColumn('MES', lpad(month('DATA_VENDA'),2,'0'))
                   #Transformação das datas
                  .groupBy('ANO','MES','LINHA').agg(sum('QTD_VENDA').alias('TOTAL_VENDAS'))
                  .orderBy('ANO','MES')
                  .select(*dfLinhaAM)
                  
                )

# COMMAND ----------

createTable = pyspark.sql.DataFrameWriter(dfLinhaAnoMes)
createTable.jdbc(url=url, table="trusted_cons_vendas_linha_ano_mes", mode ="overwrite", properties = properties)

# COMMAND ----------

selectQueryVLAM = "(select * from dbo.trusted_cons_vendas_linha_ano_mes) trusted_cons_vendas_marca_linha"
dfQueryVLAM = spark.read.jdbc(url=url,table=selectQueryVLAM,properties=properties)
