# Databricks notebook source
# MAGIC %md 
# MAGIC #### Import libs e conexões

# COMMAND ----------

from pyspark import SparkFiles
import pyspark
from pyspark.sql.functions import *
from pyspark.sql.types import *

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
dfVendasML=[
              'MARCA',
              'LINHA',
              'TOTAL_VENDAS'
         ]

# COMMAND ----------

#Cria novo dataframe padronizado, lógica de agragacao para cria a visão de vendas consolidadas por marca e linha
dfVendasMarcaLinha = ( dfSelect
                  .groupBy('MARCA','LINHA').agg(sum('QTD_VENDA').alias('TOTAL_VENDAS'))
                  .select(*dfVendasML)
            )

# COMMAND ----------

# MAGIC %md 
# MAGIC #### Cria a tabela no Azure sql server

# COMMAND ----------

createTable = pyspark.sql.DataFrameWriter(dfVendasMarcaLinha)
createTable.jdbc(url=url, table="trusted_cons_vendas_marca_linha", mode ="overwrite", properties = properties)

# COMMAND ----------

selectQueryVML = "(select * from dbo.trusted_cons_vendas_marca_linha) trusted_cons_vendas_marca_linha"
dfQueryVML = spark.read.jdbc(url=url,table=selectQueryVML,properties=properties)
