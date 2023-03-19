# Databricks notebook source
import re

# COMMAND ----------

# MAGIC %md
# MAGIC #### Function to check if the directory exists

# COMMAND ----------

def folder_exists(path):
    try:
        dbutils.fs.ls(path)
        return True
    except Exception as error:
        if 'java.io.FileNotFoundException' in str(error):
            return False 

# COMMAND ----------

# MAGIC %md
# MAGIC #### Function to get all files from a directory and subdirectory

# COMMAND ----------

def get_dir_content(ls_path):
    for dir_path in dbutils.fs.ls(ls_path):
        if dir_path.isFile():
            yield dir_path.path
        elif dir_path.isDir() and ls_path != dir_path.path:
            yield from get_dir_content(dir_path.path)

# COMMAND ----------

# MAGIC %md #### Function Clean All files & directory. Method 1

# COMMAND ----------

def clean_all(ls_path):
    try:
        for file in files_stage:
            if file.isDir() or file.isFile():
                dbutils.fs.rm(file.path, recurse=True)
        print('Clean all directory')
    except Exception as e:
        print(f'ERROR: {e}')

# COMMAND ----------

# MAGIC %md #### Function Clean All files & directory. Method 2

# COMMAND ----------

def clean(ls_path):
    try:
        for i in (ls_path):
            dbutils.fs.rm(i[0],True)
        print('Clean all directory')
    except Exception as e:
        print(f'ERROR: {e}')

# COMMAND ----------

# MAGIC %md #### Function Copy files 1

# COMMAND ----------

def copy_files(folder_stage_list, input_folder, output_folder, table):
    """
    Si existe Subdirectorios
    """    
    i=0
    try:
        # Iterate through all parquet files
        for folder in folder_stage_list:
            if not folder[1].endswith('_SUCCESS'):
                subfolder = dbutils.fs.ls(folder[0])
                folder = folder[1].replace('/','')
                for sf in subfolder:
                    if sf[1].endswith('.parquet'):
                        file = sf[1]
                        fileNamePart = file[0:10]
                        year = re.findall(r'[0-9]+',sf[0])[1][0:4]
                        period = re.findall(r'[0-9]+',sf[0])[1]

                        # If the directory does not exist, create it.
                        if not folder_exists(f'{output_folder}/{year}/{period}'):
                            dbutils.fs.mkdirs(f'{output_folder}/{year}/{period}')

                        # Copy Files
                        dbutils.fs.cp(f'dbfs:{input_folder}/{folder}/{file}', f'dbfs:{output_folder}/{year}/{period}/{fileNamePart}_{table}_{period}.parquet')
                        i+=1
        print(f'copied files: {i}')
    except Exception as e:
        print('ERROR: {e}')

# COMMAND ----------

# MAGIC %md #### Function Copy files 2

# COMMAND ----------

def copy_files_only_files(folder_stage_list, input_folder, output_folder, table):
    """
    Si no existe existe Subdirectorios
    """    
    i=0
    try:
        for file in folder_stage_list:
            if file[1].endswith('.parquet'):
                file = file[1]
                fileNamePart = file[0:10]

                # If the directory does not exist, create it.
                if not folder_exists(f'{output_folder}/'):
                    dbutils.fs.mkdirs(f'{output_folder}/')

                # Copy Files
                dbutils.fs.cp(f'dbfs:{input_folder}/{file}', f'dbfs:{output_folder}/{fileNamePart}_{table}.parquet')
                i+=1
                
        print(f'copied files: {i}')
    except Exception as e:
        print('ERROR: {e}')

# COMMAND ----------

