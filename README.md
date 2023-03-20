# Data Pipeline Azure Databricks (PySpark)

Este proyecto tiene como finalidad aplicar los conocimientos adquiridos sobre Azure, Databricks (Pyspark). Realizando un proyecto de Ingeniería de Datos.

## Arquitectura
[![Arquitectura](https://i.postimg.cc/TPCvpdvc/Arquitectura-Data-Pipeline-Azure-Databricks-Py-Spark-drawio-1.png)](https://postimg.cc/SX2vPpsX)

## Explicación en video youtube (dale click)
[![Alt text](https://img.youtube.com/vi/Qorw1uWS53Y/0.jpg)](https://www.youtube.com/watch?v=Qorw1uWS53Y)



## Escenario
La empresa AFTO necesita analizar las ventas de su operación. Para ello se realizo un Datamart sobre su Base de datos transaccional. Se utilizo los servicios de Azure para realizar el proyecto.

## Tecnologias utilizadas
- Spark (Pyspark), Databricks, Azure SQL, AWS RDS PostgreSQL, Azure Data Lake Gen2, Azure Data Factory, Azure Key Vault

## Proceso

1.- Se crean los servicios
- Azure Data Lake Gen2
- Azure Databricks
- Azure Data Factory
- Azure Key Vault
- Azure SQL

2.- Los datos se extraen de un sistema de base de datos transacctional que esta en un AWS RDS PostgreSQL
[![data-oltp-drawio.png](https://i.postimg.cc/Gh34gQb7/data-oltp-drawio.png)](https://postimg.cc/xq7jcLJM)

3.- En el Servicio de Azure Data Lake Gen2 se crean 4 zonas:
. Stage.- Es una capa temporal el cual tiene como finalidad distribuir los archivos a la zona especifica.
. Raw.- Es la zona donde se reciben los archivos en crudo
. Transformed.- Se realizar las transformaciones necesarias.
. Analytics.- Se crean las dimensiones y las tablas de hechos
[![Storage.png](https://i.postimg.cc/qMK2xgRj/Storage.png)](https://postimg.cc/56xHx48C)

4.- Para el procesamiento de Utiliza Azure databricks para los procesos de extración, carga, transformación y carga (ELTL). Se crear diferentes notebook en databricks y funciones personalizadas
[![azure-databricks.png](https://i.postimg.cc/8zDt0x78/azure-databricks.png)](https://postimg.cc/0bWY6WGZ)

5.- Para el orquestamiento y programación se utliza Azure Data Factory para orquestar los notebook creados en Azure Databricks
[![Azure-Data-Factory.png](https://i.postimg.cc/wT5kLF0w/Azure-Data-Factory.png)](https://postimg.cc/vcB9d7Rn)

6.- En Azure Key Vault se crean secretos para las diferentes credenciales de los diferentes servicios.
[![Azure-key-Vault.png](https://i.postimg.cc/P5r1ZHLq/Azure-key-Vault.png)](https://postimg.cc/kBzV3kvr)

7.- Se crea el servico de AzureSQL para crear un datamart sobre las ventas.
[![data-olap-drawio.png](https://i.postimg.cc/cJf1Lgst/data-olap-drawio.png)](https://postimg.cc/2q8NH5LC)
[![AzureSQL.png](https://i.postimg.cc/hjSn42yN/AzureSQL.png)](https://postimg.cc/XZDmQk0g)

## Contacto
- ztejorge@hotmail.com
