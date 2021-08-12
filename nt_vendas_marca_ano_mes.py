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
spark.conf.set("spark.databricks.optimizer.dynamicPartitionPruning","true")
spark.conf.set('spark.databricks.delta.retentionDurationCheck.enabled','false')
sp_timezone = pytz.timezone('America/Sao_Paulo')

# COMMAND ----------

# MAGIC %run /Users/ruan.pomponet@gmail.com/db_connetion

# COMMAND ----------

selectQuery = "(select * from dbo.raw_vendas) raw_vendas"
dfSelect = spark.read.jdbc(url=url,table=selectQuery,properties=properties)

# COMMAND ----------

dfMarcaAM = [
              'MARCA',
              'ANO',
              'MES',
              'TOTAL_VENDAS'
            ]

# COMMAND ----------

dfMarcaAnoMes = ( dfSelect
                  .withColumn('ANO', year('DATA_VENDA'))
                  .withColumn('MES', lpad(month('DATA_VENDA'),2,'0'))
                   #Transformação de datas
                  .groupBy('ANO','MES','MARCA').agg(sum('QTD_VENDA').alias('TOTAL_VENDAS'))
                  .orderBy('ANO','MES')
                  .select(*dfMarcaAM)
                  
                )

# COMMAND ----------

createTable = pyspark.sql.DataFrameWriter(dfMarcaAnoMes)
createTable.jdbc(url=url, table="trusted_cons_vendas_marca_ano_mes", mode ="overwrite", properties = properties)

# COMMAND ----------

selectQueryVMAM = "(select * from dbo.trusted_cons_vendas_marca_ano_mes) trusted_cons_vendas_marca_linha"
dfQueryVMAM = spark.read.jdbc(url=url,table=selectQueryVMAM,properties=properties)
