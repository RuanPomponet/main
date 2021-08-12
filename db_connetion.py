# Databricks notebook source
jdbcHostname = "sql-case-coticario.database.windows.net"
jdbcPort = "1433"
jdbcDatabase = "bd-sql-case-boticario"
conexaoSQLServerAzure = dbutils.secrets.get(scope="key-vault-secrets",key="conexaoSQLServerAzure")
properties = {
 "user" : "case-bot",
 "password" : conexaoSQLServerAzure }

url = f'jdbc:sqlserver://{jdbcHostname}:{jdbcPort};database={jdbcDatabase}'
