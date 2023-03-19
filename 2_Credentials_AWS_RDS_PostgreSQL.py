# Databricks notebook source
# MAGIC %md #### Connect Base De Datos AWS RDS PostgreSQL | using Azure Key Vault

# COMMAND ----------

database_host_pg = dbutils.secrets.get(scope= 'akv_secret', key='database-host-pg')
database_port_pg = "5432" 
database_name_pg = dbutils.secrets.get(scope= 'akv_secret', key='database-name-pg')
#table_pg = "public.rental" 
user_pg = dbutils.secrets.get(scope= 'akv_secret', key='user-pg')
password_pg = dbutils.secrets.get(scope= 'akv_secret', key='password-pg')

driver_pg = "org.postgresql.Driver"
url_pg = f"jdbc:postgresql://{database_host_pg}:{database_port_pg}/{database_name_pg}"

# COMMAND ----------

# MAGIC %md #### Query Custom

# COMMAND ----------


""" 
pushdown_query = "(select * from public.payment limit 5) as payment"

payment_table = (spark.read
  .format("jdbc")
  .option("url", url)
  .option("dbtable", pushdown_query)
  .option("user", user)
  .option("password", password)
  .load()
)

payment_table.printSchema()
print(payment_table.count())

display(payment_table)
"""

# COMMAND ----------

