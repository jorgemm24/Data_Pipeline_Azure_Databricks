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

# MAGIC %md #### Create DataFrame Film

# COMMAND ----------

df_film = spark.read.format('parquet').load('/mnt/adls/2_transformed/film')

#print(df_film.count())
#df_film.printSchema()

#display(df_film)

# COMMAND ----------

# MAGIC %md ####  Write Dataframe into ADLS

# COMMAND ----------

#from datetime import datetime
#outPath = '/mnt/adls/customerOutput_'+datetime.now().strftime('%Y%m%d%H%M%S')+ '/'
#customerDF.write.format('csv').option('header',True).option('sep','|').save(outPath)

df_film.repartition(1).write.format("parquet").mode("overwrite").save("/mnt/adls/0_stage")

# COMMAND ----------

# MAGIC %md #### Copy files to Transformed

# COMMAND ----------

table = 'dim_film'
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

# MAGIC %md #### Load dim_film to Azure SQL

# COMMAND ----------

table_azuresql = "dim.film" 

df_film.write \
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

del df_film

# COMMAND ----------

