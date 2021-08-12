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

selectQuery = "(select * from dbo.raw_vendas) raw_vendas"
dfSelect = spark.read.jdbc(url=url,table=selectQuery,properties=properties)

# COMMAND ----------

# MAGIC %md 
# MAGIC #### Criação do spark dataframe, definição de schema e tratamendo de dados

# COMMAND ----------

#Cria novo dataframe padronizando campos
dfVendasAM=[
              'ANO',
              'MES',
              'TOTAL_VENDAS'
         ]

# COMMAND ----------

#Cria novo dataframe padronizado, insere data types e aplica lógica de agragacao para cria a visão de vendas consolidadas por ano/mes
dfVendasAnoMes = ( dfSelect
                  .withColumn('DATA_VENDA',to_date(col('DATA_VENDA'),'dd/MM/yyyy'))
                  .withColumn('ANO', year('DATA_VENDA'))
                  .withColumn('MES', lpad(month('DATA_VENDA'),2,'0'))
                  #Tratamento de datas
                  .withColumn('QTD_VENDA', col('QTD_VENDA').cast('int'))
                  .groupBy('MES','ANO').agg(sum('QTD_VENDA').alias('TOTAL_VENDAS'))
                  .orderBy('ANO','MES')
                  .select(*dfVendasAM)
            )

# COMMAND ----------

# MAGIC %md 
# MAGIC #### Cria a tabela no Azure sql server

# COMMAND ----------

createTable = pyspark.sql.DataFrameWriter(dfVendasAnoMes)
createTable.jdbc(url=url, table="trusted_cons_vendas_ano_mes", mode ="overwrite", properties = properties)

# COMMAND ----------

selectQueryVAM = "(select * from dbo.trusted_cons_vendas_ano_mes) trusted_cons_vendas_aa_mm"
dfQueryVAM = spark.read.jdbc(url=url,table=selectQuery,properties=properties)
