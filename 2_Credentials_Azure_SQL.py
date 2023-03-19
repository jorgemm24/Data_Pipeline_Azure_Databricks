# Databricks notebook source
# MAGIC %md #### Connect Base De Datos Azure SQL| using Azure Key Vault

# COMMAND ----------

try:
    database_host_azuresql = dbutils.secrets.get(scope= 'akv_secret', key='jdbcHostName')
    database_port_azuresql = 1433
    database_name_azuresql = dbutils.secrets.get(scope= 'akv_secret', key='db-name-azure-afto')
    #table_azuresql = "dim.store" 
    user_azuresql = dbutils.secrets.get(scope= 'akv_secret', key='jdbcUsername')
    password_azuresql = dbutils.secrets.get(scope= 'akv_secret', key='jdbcPassword')

    driver_azuresql = "com.microsoft.sqlserver.jdbc.SQLServerDriver"
    url_azuresql = f'jdbc:sqlserver://{database_host_azuresql}:{database_port_azuresql};databaseName={database_name_azuresql};user={user_azuresql};password={password_azuresql}'
except Exception as e:
    print(e)
    


# COMMAND ----------

