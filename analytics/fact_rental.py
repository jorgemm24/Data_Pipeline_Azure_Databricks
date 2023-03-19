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

#ls '/mnt/adls'

# COMMAND ----------

# MAGIC %md #### Create DataFrame Rental

# COMMAND ----------

# Then dynamically process only the current period

df_rental = spark.read.format('parquet').option('recursiveFileLookup', 'true').load('/mnt/adls/2_transformed/rental/')

print(f'rows: {df_rental.count()}')
df_rental.printSchema()

display(df_rental)

# COMMAND ----------

# MAGIC %md #### Create DataFrame Payment

# COMMAND ----------

# Then dynamically process only the current period

df_payment = spark.read.format('parquet').option('recursiveFileLookup', 'true').load('/mnt/adls/2_transformed/payment/')

print(f'rows: {df_payment.count()}')
df_payment.printSchema()

display(df_payment)

# COMMAND ----------

# MAGIC %md #### Create Dataframe Film

# COMMAND ----------

df_film = spark.read.format('parquet').option('recursiveFileLookup', 'true').load('/mnt/adls/2_transformed/film/')

print(df_film.count())
display(df_film)

# COMMAND ----------

# MAGIC %md #### Create Dataframe Inventory

# COMMAND ----------

df_inventory = spark.read.format('parquet').option('recursiveFileLookup', 'true').load('/mnt/adls/2_transformed/inventory/')

print(df_inventory.count())
display(df_inventory)

# COMMAND ----------

# MAGIC %md #### Create DataFrame Store

# COMMAND ----------

df_store = spark.read.format('parquet').option('recursiveFileLookup', 'true').load('/mnt/adls/2_transformed/store/')

print(df_store.count())
display(df_store)

# COMMAND ----------

# MAGIC %md #### Create DataFrame Staff

# COMMAND ----------

df_staff = spark.read.format('parquet').option('recursiveFileLookup', 'true').load('/mnt/adls/2_transformed/staff/')

print(df_staff.count())
display(df_staff)

# COMMAND ----------

# MAGIC %md #### Create Dataframe FactRental

# COMMAND ----------

from pyspark.sql.functions import col, date_format, to_date, sum

df_fact_rental = df_rental.alias('rental').join(df_payment.alias('payment'), col('rental.rental_id')==col('payment.rental_id'), 'left')\
                                           .join(df_staff.alias('staff'), col('payment.staff_id')==col('staff.staff_id'), 'left')\
                                          .join(df_store.alias('store'), col('staff.store_id')==col('store.store_id'), 'left')\
                                          .join(df_inventory.alias('inventory'), col('rental.inventory_id')==col('inventory.inventory_id'), 'left' ) \
                    .select( 
                             date_format(col('rental.rental_date'), 'yyyMMdd').alias('date_id')
                            ,date_format(col('rental.rental_date'),'HH:00').alias('hour') 
                            ,col('rental.customer_id')
                            ,col('store.store_id')
                            ,col('inventory.film_id')
                            ,col('payment.amount')
                            ,date_format(col('rental.rental_date'), 'yyyMM').alias('period')
                           )

 

print(df_fact_rental.count())
df_fact_rental.printSchema()

display(df_fact_rental)

# COMMAND ----------

# MAGIC %md #### Aggregate

# COMMAND ----------

df_fact_rental_t = df_fact_rental.groupBy(col('date_id'), col('hour'), col('customer_id'), col('store_id'), col('film_id'), col('period')).agg(sum(col('amount')).alias('amount'))\
                                    .fillna({'amount':'0'})

print(df_fact_rental_t.count())
df_fact_rental_t.printSchema()
display(df_fact_rental_t)

df_fact_rental_t_load = df_fact_rental_t.select(col('date_id'), col('hour'), col('customer_id'), col('store_id'), col('film_id'),col('amount'))

# COMMAND ----------

# MAGIC %md ####  Write Dataframe into ADLS

# COMMAND ----------

#from datetime import datetime
#outPath = '/mnt/adls/customerOutput_'+datetime.now().strftime('%Y%m%d%H%M%S')+ '/'
#customerDF.write.format('csv').option('header',True).option('sep','|').save(outPath)

df_fact_rental_t.repartition(3).write.format("parquet").partitionBy("period").mode("overwrite").save("/mnt/adls/0_stage")


# COMMAND ----------

# MAGIC %md #### Copy files to Raw per Period

# COMMAND ----------

table = 'fact_rental'
zone = '3_analytics'
folder_stage_list = dbutils.fs.ls("/mnt/adls/0_stage")
input_folder = '/mnt/adls/0_stage'
output_folder = f'/mnt/adls/{zone}/{table}'


copy_files(folder_stage_list, input_folder, output_folder, table)

# COMMAND ----------

# MAGIC %md #### Clean All files & directory

# COMMAND ----------

clean(ls_path=folder_stage_list)

# COMMAND ----------

# MAGIC %md #### Load fact_rental to Azure SQL

# COMMAND ----------

table_azuresql = "fact.rental" 

df_fact_rental_t_load.write \
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

del df_rental, df_payment, df_film, df_inventory, df_store, df_fact_rental, df_fact_rental_t, df_fact_rental_t_load

# COMMAND ----------

