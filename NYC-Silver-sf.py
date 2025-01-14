# Databricks notebook source
# MAGIC %md
# MAGIC ## Access

# COMMAND ----------



# COMMAND ----------

dbutils.fs.ls("abfss://bronze@nyctaxicesa.dfs.core.windows.net/")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Data Reading
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Importing Libraries

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

# MAGIC %md
# MAGIC #### Reading CSV Data 

# COMMAND ----------

# MAGIC %md 
# MAGIC ##### Trip Type Data && Trip Zone

# COMMAND ----------

df_trip_type = spark.read.format("csv")\
                    .option("header", "true")\
                    .option("inferSchema", "true")\
                    .load("abfss://bronze@nyctaxicesa.dfs.core.windows.net/trip_type")

# COMMAND ----------

display(df_trip_type)

# COMMAND ----------

df_trip_zone = spark.read.format("csv")\
                    .option("header", "true")\
                    .option("inferSchema", "true")\
                    .load("abfss://bronze@nyctaxicesa.dfs.core.windows.net/trip_zone")

# COMMAND ----------

df_trip_zone.display()

# COMMAND ----------

# MAGIC %md 
# MAGIC #### Trip Data 

# COMMAND ----------

myschema = '''
                VendorID BIGINT,
                lpep_pickup_datetime TIMESTAMP,
                lpep_dropoff_datetime TIMESTAMP,
                store_and_fwd_flag STRING,
                RatecodeID BIGINT,
                PULocationID BIGINT,
                DOLocationID BIGINT,
                passenger_count BIGINT,
                trip_distance DOUBLE,
                fare_amount DOUBLE,
                extra DOUBLE,
                mta_tax DOUBLE,
                tip_amount DOUBLE,
                tolls_amount DOUBLE,
                ehail_fee DOUBLE,
                improvement_surcharge DOUBLE,
                total_amount DOUBLE,
                payment_type BIGINT,
                trip_type BIGINT,
                congestion_surcharge DOUBLE
'''


# COMMAND ----------

df_trip_2023 = spark.read.format("parquet")\
                    .option("header", "true")\
                    .schema(myschema)\
                    .option("recursive", "true")\
                    .load("abfss://bronze@nyctaxicesa.dfs.core.windows.net/trip2023data/trip-data")

# COMMAND ----------

df_trip_2023.display()

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Data Transforamtion

# COMMAND ----------

# MAGIC %md 
# MAGIC ##### Trip Type Data && Trip Zone

# COMMAND ----------

df_trip_type.display()

# COMMAND ----------

df_trip_type.write.format("parquet")\
            .mode("overwrite")\
            .save("abfss://silver@nyctaxicesa.dfs.core.windows.net/trip-data")

# COMMAND ----------

df_trip_zone.display()

# COMMAND ----------

df_T_trip_zone  = df_trip_zone.withColumn('zoen1' , split(col('Zone'), "/")[0])\
                              .withColumn('zoen2' , split(col('Zone'), "/")[1])

df_T_trip_zone.display()

# COMMAND ----------

df_T_trip_zone.write.format("parquet")\
            .mode("overwrite")\
            .save("abfss://silver@nyctaxicesa.dfs.core.windows.net/trip-zone")

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Trip 2023 Data

# COMMAND ----------

df_trip_2023.display()

# COMMAND ----------

df_T_trip_2023 = df_trip_2023.withColumn("trip_date" , to_date ("lpep_dropoff_datetime"))\
                                .withColumn("trip_year"  , year ("lpep_dropoff_datetime"))\
                                .withColumn("trip_month", month ("lpep_dropoff_datetime"))

# COMMAND ----------

display(df_T_trip_2023)

# COMMAND ----------

print(df_T_trip_2023.columns)


# COMMAND ----------

df_T_trip_2023 = df_T_trip_2023.select(
    "total_amount",
    "VendorID",
    "fare_amount",
    "PULocationID",
    "DOLocationID",

)

display(df_T_trip_2023)

# COMMAND ----------

df_T_trip_2023.printSchema()

# COMMAND ----------

df_T_trip_2023.write.format("parquet")\
            .mode("overwrite")\
            .save("abfss://silver@nyctaxicesa.dfs.core.windows.net/trip-data-2023")

# COMMAND ----------

# MAGIC %md 
# MAGIC # Data Analysis

# COMMAND ----------

display(df_T_trip_2023)

# COMMAND ----------

