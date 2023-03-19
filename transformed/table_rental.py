# Databricks notebook source
# MAGIC %md #### Importing custom Functions

# COMMAND ----------

# MAGIC %run ../0_Functions

# COMMAND ----------

#%fs

#ls '/mnt/adls'

# COMMAND ----------

# MAGIC %md #### Create DataFrame Rental

# COMMAND ----------

# Then dynamically process only the current period

df_rental = spark.read.format('parquet').option('recursiveFileLookup', 'true').load('/mnt/adls/1_raw/rental/')

print(f'rows: {df_rental.count()}')
df_rental.printSchema()

#display(df_rental)

# COMMAND ----------

# MAGIC %md #### Transformed

# COMMAND ----------

from pyspark.sql.functions import col, date_format, to_date

df_rental_t = df_rental.select( 
                                 col('rental_id')
                                ,col('rental_date')
                                ,date_format(col('rental_date'), 'yyyMMdd').alias('date_id')
                                ,to_date(col('rental_date'),'yyyy-MM-dd').alias('date')
                                ,date_format(col('rental_date'),'HH:00').alias('hour') 
                                ,col('inventory_id')
                                ,col('customer_id')
                                ,date_format(col('rental_date'), 'yyyMM').alias('period')
                               )

   
#print(df_rental_t.count())
#df_rental_t.printSchema()

#display(df_rental_t)

# COMMAND ----------

# MAGIC %md ####  Write Dataframe into ADLS

# COMMAND ----------

#from datetime import datetime
#outPath = '/mnt/adls/customerOutput_'+datetime.now().strftime('%Y%m%d%H%M%S')+ '/'
#customerDF.write.format('csv').option('header',True).option('sep','|').save(outPath)

df_rental_t.repartition(3).write.format("parquet").partitionBy("period").mode("overwrite").save("/mnt/adls/0_stage")


# COMMAND ----------

# MAGIC %md #### Copy files to Raw per Period

# COMMAND ----------

table = 'rental'
zone = '2_transformed'
folder_stage_list = dbutils.fs.ls("/mnt/adls/0_stage")
input_folder = '/mnt/adls/0_stage'
output_folder = f'/mnt/adls/{zone}/{table}'

copy_files(folder_stage_list, input_folder, output_folder, table)

# COMMAND ----------

# MAGIC %md #### Clean All files & directory

# COMMAND ----------

clean(ls_path=folder_stage_list)

# COMMAND ----------

# MAGIC %md #### Deleted

# COMMAND ----------

del df_rental, df_rental_t

# COMMAND ----------

