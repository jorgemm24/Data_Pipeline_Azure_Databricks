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

df_dim_date = spark.read.format('parquet').option('recursiveFileLookup', 'true').load('/mnt/adls/1_raw/date/')

print(f'rows: {df_dim_date.count()}')
df_dim_date.printSchema()

display(df_dim_date)

# COMMAND ----------

# MAGIC %md ####  Write Dataframe into ADLS

# COMMAND ----------

#from datetime import datetime
#outPath = '/mnt/adls/customerOutput_'+datetime.now().strftime('%Y%m%d%H%M%S')+ '/'
#customerDF.write.format('csv').option('header',True).option('sep','|').save(outPath)

df_dim_date.repartition(1).write.format("parquet").mode("overwrite").save("/mnt/adls/0_stage")


# COMMAND ----------

# MAGIC %md #### Copy files to Raw per Period

# COMMAND ----------

table = 'dim_date'
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

# MAGIC %md #### Load dim_date to Azure SQL

# COMMAND ----------

table_azuresql = "dim.date" 

df_dim_date.write \
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

del df_dim_date

# COMMAND ----------

 