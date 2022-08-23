# Databricks notebook source
dbutils.widgets.removeAll()
dbutils.widgets.text("read_file_location", "dbfs:/FileStore/tables/sample_data-2.csv")
dbutils.widgets.text("write_file_location", "dbfs:/FileStore/tables/write")

# COMMAND ----------



# COMMAND ----------

from pyspark.sql.types import StructType, IntegerType, TimestampType
from pyspark.sql.functions import *


read_file_location = dbutils.widgets.get("read_file_location")
write_file_location = dbutils.widgets.get("write_file_location")
schema = StructType() \
      .add("Period",IntegerType(),True) \
      .add("Volume",IntegerType(),True) #\
      #.add("input_date",TimestampType(),True)
file_date = "2015-01-02"

df = spark.read.option("header", False).option("delimiter", "\t").schema(schema).csv(read_file_location)
#   df = df.withColumn("file_name", input_file_name())
df = df.withColumn("input_date", to_date(lit(file_date)))
display(df)
df = df.withColumn("StartPeriod", when(((col("Period") - 2) % 24) == -1, 23).otherwise((col("Period") - 2) % 24)).drop("Period")
df = df.groupBy("input_date", "StartPeriod").agg(sum("Volume").alias("Volume"))
df = df.select(col("input_date"), concat(lpad(col("StartPeriod"), 2, '0'), lit(":00")).alias('Period'), "Volume" )
display(df)
df.write.format("delta").mode("overwrite").partitionBy("input_date").save(write_file_location) #  .partitionBy()  - input date

