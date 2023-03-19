# Databricks notebook source
# MAGIC %md #### Importing custom Functions

# COMMAND ----------

# MAGIC %run ../0_Functions

# COMMAND ----------

#%fs 
#ls '/mnt/adls/'

# COMMAND ----------

# MAGIC %md #### Create DataFrame City

# COMMAND ----------

df_city = spark.read.format('parquet').load('/mnt/adls/1_raw/city')

#print(df_city.count())
#df_city.printSchema()

#display(df_city)

# COMMAND ----------

# MAGIC %md #### Transformed

# COMMAND ----------

from pyspark.sql.functions import col

df_city_t = df_city.select( col('city_id') ,col('city') )

#print(df_city_t.count())
#df_city_t.printSchema()

#display(df_city_t)

# COMMAND ----------

# MAGIC %md ####  Write Dataframe into ADLS

# COMMAND ----------

#from datetime import datetime
#outPath = '/mnt/adls/customerOutput_'+datetime.now().strftime('%Y%m%d%H%M%S')+ '/'
#customerDF.write.format('csv').option('header',True).option('sep','|').save(outPath)

df_city_t.repartition(1).write.format("parquet").mode("overwrite").save("/mnt/adls/0_stage")

# COMMAND ----------

# MAGIC %md #### Copy files to Transformed

# COMMAND ----------

table = 'city'
zone = '2_transformed'
folder_stage_list = dbutils.fs.ls("/mnt/adls/0_stage")
input_folder = '/mnt/adls/0_stage'
output_folder = f'/mnt/adls/{zone}/{table}'

copy_files_only_files(folder_stage_list, input_folder, output_folder, table)

# COMMAND ----------

# MAGIC %md #### Clean All files & directory

# COMMAND ----------

clean(ls_path=folder_stage_list)

# COMMAND ----------

# MAGIC %md #### Deleted

# COMMAND ----------

del df_city, df_city_t

# COMMAND ----------

