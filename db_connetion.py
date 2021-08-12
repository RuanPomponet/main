# Databricks notebook source
jdbcHostname = "sql-case-coticario.database.windows.net"
jdbcPort = "1433"
jdbcDatabase = "bd-sql-case-boticario"
properties = {
 "user" : "case-bot",
 "password" : "Karazu@89" }

url = "jdbc:sqlserver://{0}:{1};database={2}".format(jdbcHostname,jdbcPort,jdbcDatabase)
