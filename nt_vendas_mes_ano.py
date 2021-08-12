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
#Boticario_case89

# COMMAND ----------

# MAGIC %run /Users/ruan.pomponet@gmail.com/db_connetion

# COMMAND ----------

selectQuery = "(select * from dbo.raw_vendas) raw_vendas"
dfSelect = spark.read.jdbc(url=url,table=selectQuery,properties=properties)

# COMMAND ----------

dfVendasAM=[
              'ANO',
              'MES',
              'TOTAL_VENDAS'
         ]

# COMMAND ----------

dfVendasAnoMes = ( dfSelect
                  .withColumn('DATA_VENDA',to_date(col('DATA_VENDA'),'dd/MM/yyyy'))
                  .withColumn('ANO', year('DATA_VENDA'))
                  .withColumn('MES', lpad(month('DATA_VENDA'),2,'0'))
                  .withColumn('QTD_VENDA', col('QTD_VENDA').cast('int'))
                  .groupBy('MES','ANO').agg(sum('QTD_VENDA').alias('TOTAL_VENDAS'))
                  .orderBy('ANO','MES')
                  .select(*dfVendasAM)
            )

# COMMAND ----------

createTable = pyspark.sql.DataFrameWriter(dfVendasAnoMes)
createTable.jdbc(url=url, table="trusted_cons_vendas_ano_mes", mode ="overwrite", properties = properties)

# COMMAND ----------

selectQueryVAM = "(select * from dbo.trusted_cons_vendas_ano_mes) trusted_cons_vendas_aa_mm"
dfQueryVAM = spark.read.jdbc(url=url,table=selectQuery,properties=properties)
