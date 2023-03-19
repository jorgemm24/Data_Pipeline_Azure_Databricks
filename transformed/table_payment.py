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

df_payment = spark.read.format('parquet').option('recursiveFileLookup', 'true').load('/mnt/adls/1_raw/payment/')

print(f'rows: {df_payment.count()}')
df_payment.printSchema()

#display(df_payment)

# COMMAND ----------

# MAGIC %md #### Transformed

# COMMAND ----------

from pyspark.sql.functions import col, date_format, to_date

df_payment_t = df_payment.select( 
                                 col('payment_id')
                                ,col('customer_id')
                                ,col('staff_id')
                                ,col('rental_id')
                                ,col('amount')
                                ,col('rental_date')
                                ,date_format(col('rental_date'), 'yyyMM').alias('period')
                               )
   
print(df_payment_t.count())
df_payment_t.printSchema()

#display(df_payment_t)

# COMMAND ----------

# MAGIC %md ####  Write Dataframe into ADLS

# COMMAND ----------

#from datetime import datetime
#outPath = '/mnt/adls/customerOutput_'+datetime.now().strftime('%Y%m%d%H%M%S')+ '/'
#customerDF.write.format('csv').option('header',True).option('sep','|').save(outPath)

df_payment_t.repartition(3).write.format("parquet").partitionBy("period").mode("overwrite").save("/mnt/adls/0_stage")

# COMMAND ----------

# MAGIC %md #### Copy files to Raw per Period

# COMMAND ----------

table = 'payment'
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

del df_payment, df_payment_t

# COMMAND ----------

