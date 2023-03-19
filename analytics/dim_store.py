# Databricks notebook source
# MAGIC %md #### Importing custom Functions

# COMMAND ----------

# MAGIC %run ../0_Functions

# COMMAND ----------

# MAGIC %md #### Credentials Azure SQL

# COMMAND ----------

# MAGIC %run ../2_Credentials_Azure_SQL

# COMMAND ----------

#%fs 
#ls '/mnt/adls/'

# COMMAND ----------

# MAGIC %md #### Create DataFrame Customer

# COMMAND ----------

df_store = spark.read.format('parquet').load('/mnt/adls/2_transformed/store')

#print(df_store.count())
#df_store.printSchema()

#display(df_store)

# COMMAND ----------

# MAGIC %md #### Create DataFrame Address

# COMMAND ----------

df_address = spark.read.format('parquet').load('/mnt/adls/2_transformed/address')

#print(df_address.count())
#df_address.printSchema()

#display(df_address)

# COMMAND ----------

# MAGIC %md #### Create DataFrame City

# COMMAND ----------

df_city = spark.read.format('parquet').load('/mnt/adls/2_transformed/city')

#print(df_city.count())
#df_city.printSchema()

#display(df_city)

# COMMAND ----------

# MAGIC %md #### Create DataFrame dim_store

# COMMAND ----------

from pyspark.sql.functions import col

df_dim_store = df_store.alias('store').join(df_address.alias('address'), col('store.address_id')==col('address.address_id'), 'left')\
                                  .join(df_city.alias('city'), col('address.city_id')==col('city.city_id') )\
                                  .select( col('store.store_id'), col('address.address'), col('address.district'), col('city.city')  )

#df_dim_store.printSchema()
#display(df_dim_store)
                                    

# COMMAND ----------

# MAGIC %md ####  Write Dataframe into ADLS

# COMMAND ----------

#from datetime import datetime
#outPath = '/mnt/adls/customerOutput_'+datetime.now().strftime('%Y%m%d%H%M%S')+ '/'
#customerDF.write.format('csv').option('header',True).option('sep','|').save(outPath)

df_dim_store.repartition(1).write.format("parquet").mode("overwrite").save("/mnt/adls/0_stage")

# COMMAND ----------

# MAGIC %md #### Copy files to Transformed

# COMMAND ----------

table = 'dim_store'
zone = '3_analytics'
folder_stage_list = dbutils.fs.ls("/mnt/adls/0_stage")
input_folder = '/mnt/adls/0_stage'
output_folder = f'/mnt/adls/{zone}/{table}'


copy_files_only_files(folder_stage_list, input_folder, output_folder, table)

# COMMAND ----------

# MAGIC %md #### Clean All files & directory

# COMMAND ----------

clean(ls_path=folder_stage_list)

# COMMAND ----------

# MAGIC %md #### Load dim_store to Azure SQL

# COMMAND ----------

table_azuresql = "dim.store" 

df_dim_store.write \
.format("jdbc") \
.mode("append") \
.option("url", url_azuresql) \
.option("dbtable", table_azuresql) \
.option("user", user_azuresql) \
.option("password", password_azuresql) \
.save()

# COMMAND ----------

# MAGIC %md #### Deleted

# COMMAND ----------

del df_store, df_address, df_city

# COMMAND ----------

