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

dfVendasML=[
              'MARCA',
              'LINHA',
              'TOTAL_VENDAS'
         ]

# COMMAND ----------

dfVendasMarcaLinha = ( dfSelect
                  .groupBy('MARCA','LINHA').agg(sum('QTD_VENDA').alias('TOTAL_VENDAS'))
                  .select(*dfVendasML)
            )

# COMMAND ----------

createTable = pyspark.sql.DataFrameWriter(dfVendasMarcaLinha)
createTable.jdbc(url=url, table="trusted_cons_vendas_marca_linha", mode ="overwrite", properties = properties)

# COMMAND ----------

selectQueryVML = "(select * from dbo.trusted_cons_vendas_marca_linha) trusted_cons_vendas_marca_linha"
dfQueryVML = spark.read.jdbc(url=url,table=selectQueryVML,properties=properties)
