# Databricks notebook source
# MAGIC %md #### Importing custom Functions

# COMMAND ----------

# MAGIC %run ../0_Functions

# COMMAND ----------

#%run ./1_Mount_Azure_DataLake

# COMMAND ----------

# MAGIC %run ../2_Credentials_AWS_RDS_PostgreSQL

# COMMAND ----------

# MAGIC %md #### Primero todo los registro de la tabla

# COMMAND ----------

query_payment_pg = "(select a.*, b.rental_date from public.payment a left join public.rental b on a.rental_id = b.rental_id ) as payment"

remote_table = (spark.read
  .format("jdbc")
  .option("driver", driver_pg)
  .option("url", url_pg)
  .option("dbtable", query_payment_pg )
  .option("user", user_pg)
  .option("password", password_pg)
  .load()
)

print(f'rows: {remote_table.count()}')
remote_table.printSchema()

#remote_table.show(truncate=False)

# COMMAND ----------

# MAGIC %md #### Add Colum: Period

# COMMAND ----------

from pyspark.sql.functions import col,date_format

remote_table_df = remote_table.withColumn('periodo', date_format('rental_date','yyyyMM'))
#remote_table_df.show(truncate=False)

# COMMAND ----------

#%fs 
#ls '/mnt/adls/'

# COMMAND ----------

# MAGIC %md ####  Write Dataframe into ADLS

# COMMAND ----------

#from datetime import datetime
#outPath = '/mnt/adls/customerOutput_'+datetime.now().strftime('%Y%m%d%H%M%S')+ '/'
#customerDF.write.format('csv').option('header',True).option('sep','|').save(outPath)

remote_table_df.repartition(3).write.format("parquet").partitionBy("periodo").mode("overwrite").save("/mnt/adls/0_stage")


# COMMAND ----------

# MAGIC %md #### Copy files to Raw per Period

# COMMAND ----------

table = 'payment'
folder_stage_list = dbutils.fs.ls("/mnt/adls/0_stage")
input_folder = '/mnt/adls/0_stage'
output_folder = f'/mnt/adls/1_raw/payment'


copy_files(folder_stage_list, input_folder, output_folder, table)

# COMMAND ----------

# MAGIC %md #### Clean All files & directory

# COMMAND ----------

clean(ls_path=folder_stage_list)

# COMMAND ----------

# MAGIC %md #### Deleted

# COMMAND ----------

del remote_table_df
del remote_table

# COMMAND ----------

