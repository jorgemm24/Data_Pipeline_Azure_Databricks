# Databricks notebook source
# MAGIC %md #### Check Mount

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

# MAGIC %md ####  Creating Mount Point using ADLS 

# COMMAND ----------

def mount(container, storageaccount, accesskey, str_mount_point):
    try:
        dbutils.fs.mount(
                        source = f'wasbs://{container}@{storageaccount}.blob.core.windows.net',          
                        mount_point = '/mnt/adls',
                        extra_configs = {f'fs.azure.account.key.{storageaccount}.blob.core.windows.net':f'{accesskey}'}
                        )
        print('mount OK')
    except Exception as e:
        print(f'ERROR: {e}')

# COMMAND ----------

# MAGIC %md ####  Creating Mount | Using Key Vault

# COMMAND ----------

container = dbutils.secrets.get(scope='akv_secret', key='container-name')
storageaccount = dbutils.secrets.get(scope='akv_secret', key='storage-account-name')
accesskey = dbutils.secrets.get(scope='akv_secret', key='accesskey-name')
str_mount_point = '/mnt/adls'

if not check_mount(str_mount_point):
    mount(container, storageaccount, accesskey, str_mount_point)
else:
    print('Is mounted')

# COMMAND ----------

