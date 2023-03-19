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

film = 'public.category'

remote_table = (spark.read
  .format("jdbc")
  .option("driver", driver_pg)
  .option("url", url_pg)
  .option("dbtable", film )
  .option("user", user_pg)
  .option("password", password_pg)
  .load()
)

print(f'rows: {remote_table.count()}')
remote_table.printSchema()

#display(remote_table)

# COMMAND ----------

#%fs 
#ls '/mnt/adls/'

# COMMAND ----------

# MAGIC %md ####  Write Dataframe into ADLS

# COMMAND ----------

#from datetime import datetime
#outPath = '/mnt/adls/customerOutput_'+datetime.now().strftime('%Y%m%d%H%M%S')+ '/'
#customerDF.write.format('csv').option('header',True).option('sep','|').save(outPath)

remote_table.repartition(1).write.format("parquet").mode("overwrite").save("/mnt/adls/0_stage")

# COMMAND ----------

# MAGIC %md #### Copy files to Raw per Period

# COMMAND ----------

table = 'category'
folder_stage_list = dbutils.fs.ls("/mnt/adls/0_stage")
input_folder = '/mnt/adls/0_stage'
output_folder = f'/mnt/adls/1_raw/{table}'


copy_files_only_files(folder_stage_list, input_folder, output_folder, table)

# COMMAND ----------

# MAGIC %md #### Clean All files & directory

# COMMAND ----------

#input_folder = '/mnt/adls/0_stage/'
#clean_all(ls_path=input_folder)
clean(ls_path=folder_stage_list)

# COMMAND ----------

# MAGIC %md #### Deleted

# COMMAND ----------

del remote_table

# COMMAND ----------

