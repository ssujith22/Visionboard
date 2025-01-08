# Databricks notebook source
def jdbc_connect_azure_sql():
  server=dbutils.secrets.get(scope='keyvaultdatabricksvision', key='sqlserver') 
  db=dbutils.secrets.get(scope='keyvaultdatabricksvision', key='sqldb')
  user='dev_access_data'
  pwd=dbutils.secrets.get(scope='keyvaultdatabricksvision', key='sqlkeyvault')

  connectionProperties = {
    "user" : user,
    "password" : pwd,
    "driver" : "com.microsoft.sqlserver.jdbc.SQLServerDriver"
  }
  jdbcPort = 1433
  jdbcUrl= "jdbc:sqlserver://{0}:{1};database={2}".format(server, jdbcPort, db)
  return jdbcUrl,connectionProperties