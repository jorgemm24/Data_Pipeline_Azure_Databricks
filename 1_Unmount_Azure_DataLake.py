# Databricks notebook source
# MAGIC %md ####  Creating UnMount Point using ADLS 

# COMMAND ----------

def check_mount(str_mount_point):
    try:
        if any(mount.mountPoint == str_mount_point for mount in dbutils.fs.mounts()):
            return True
        else:
            return False
    except Exception as e:
        print(f'ERROR: {e}')

# COMMAND ----------

# MAGIC %md ####  Unmount Point using ADLS

# COMMAND ----------

str_mount_point = '/mnt/adls'

if check_mount(str_mount_point):
    dbutils.fs.unmount(str_mount_point)
    print('unmount OK')
else:
    print('Not mount')