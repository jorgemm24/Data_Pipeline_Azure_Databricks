# Databricks notebook source
# MAGIC %md #### Importing custom Functions

# COMMAND ----------

# MAGIC %run ../0_Functions

# COMMAND ----------

#%fs 
#ls '/mnt/adls/'

# COMMAND ----------

# MAGIC %md #### Create DataFrame Film

# COMMAND ----------

df_film = spark.read.format('parquet').load('/mnt/adls/1_raw/film')

print(df_film.count())
df_film.printSchema()

#display(df_film)

# COMMAND ----------

# MAGIC %md #### Transformed

# COMMAND ----------

from pyspark.sql.functions import col

df_film_t = df_film.select(  col('film_id') ,col('title') ,col('description') ,col('release_year') ,col('rental_rate') ,col('length')
                            ,col('replacement_cost') ,col('rating')
                          )

#display(df_film_t)

# COMMAND ----------

# MAGIC %md ####  Write Dataframe into ADLS

# COMMAND ----------

#from datetime import datetime
#outPath = '/mnt/adls/customerOutput_'+datetime.now().strftime('%Y%m%d%H%M%S')+ '/'
#customerDF.write.format('csv').option('header',True).option('sep','|').save(outPath)

df_film_t.repartition(1).write.format("parquet").mode("overwrite").save("/mnt/adls/0_stage")

# COMMAND ----------

# MAGIC %md #### Copy files to Transformed

# COMMAND ----------

table = 'film'
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

del df_film, df_film_t

# COMMAND ----------

