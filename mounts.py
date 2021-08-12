# Databricks notebook source
configs = {"fs.azure.account.auth.type": "OAuth",
           "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
           "fs.azure.account.oauth2.client.id": "c0715e50-4d2c-4fa5-89e6-a32725c37936", 
           "fs.azure.account.oauth2.client.secret": dbutils.secrets.get(scope="key-vault-secrets",key="databricksToke"), 
           "fs.azure.account.oauth2.client.endpoint": "https://login.microsoftonline.com/5838ce72-2e3a-48b3-a625-9014d46c1362/oauth2/token"}

# COMMAND ----------

dbutils.fs.mount( 
  source = "abfss://ingestion@dlcaseboticario.dfs.core.windows.net/", 
  mount_point = "/mnt/ingestion", 
  extra_configs = configs)
