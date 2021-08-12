# Databricks notebook source
# MAGIC %md 
# MAGIC #### Import libs e conexões

# COMMAND ----------

from pyspark import SparkFiles
import pyspark
from pyspark.sql.functions import *
from pyspark.sql.types import *
import pandas as pd
import sys

# COMMAND ----------

#Executa o notebook resposável pela conexão com azure sql server

# COMMAND ----------

# MAGIC %run /Repos/ruan.pomponet@gmail.com/git-bricks-case/db_connetion

# COMMAND ----------

# MAGIC %md 
# MAGIC #### Cria o dataframe com os dados da tabela das vendas para manipulação

# COMMAND ----------

selectQuery = "(select * from dbo.raw_vendas ) raw_vendas"
dfSelect = spark.read.jdbc(url=url,table=selectQuery,properties=properties)

# COMMAND ----------

# MAGIC %md 
# MAGIC #### Criação do spark dataframe, definição de schema e tratamendo de dados

# COMMAND ----------

#Cria novo dataframe padronizando campos
dfLinhaAM = [
              'LINHA',
              'ANO',
              'MES',
              'TOTAL_VENDAS'
            ]

# COMMAND ----------

#Cria novo dataframe padronizado, insere data types e aplica lógica de agragacao para cria a visão de vendas consolidadas por ano/mes e linha
dfLinhaAnoMes = ( dfSelect
                  .withColumn('ANO', year('DATA_VENDA'))
                  .withColumn('MES', lpad(month('DATA_VENDA'),2,'0'))
                   #Transformação das datas
                  .groupBy('ANO','MES','LINHA').agg(sum('QTD_VENDA').alias('TOTAL_VENDAS'))
                  .orderBy('ANO','MES')
                  .select(*dfLinhaAM)
                  
                )

# COMMAND ----------

# MAGIC %md 
# MAGIC #### Cria a tabela no Azure sql server

# COMMAND ----------

createTable = pyspark.sql.DataFrameWriter(dfLinhaAnoMes)
createTable.jdbc(url=url, table="trusted_cons_vendas_linha_ano_mes", mode ="overwrite", properties = properties)

# COMMAND ----------

selectQueryVLAM = "(select * from dbo.trusted_cons_vendas_linha_ano_mes) trusted_cons_vendas_marca_linha"
dfQueryVLAM = spark.read.jdbc(url=url,table=selectQueryVLAM,properties=properties)
