from pyspark.sql.functions import *
from pyspark.sql.session import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, LongType

spark = SparkSession.builder.master('local').getOrCreate()
df = spark.read.format("json").load("D:/Santhu_practice/practice/Spark-The-Definitive-Guide/data/flight-data/json/2010-summary.json")
myManualSchema = StructType([
    StructField("DEST_COUNTRY_NAME",StringType(), True),
      StructField("ORIGIN_COUNTRY_NAME",StringType(), True),
      StructField("count", LongType(), False)])

df = spark.read.format("json").schema(myManualSchema)\
    .load("D:/Santhu_practice/practice/Spark-The-Definitive-Guide/data/flight-data/json/2010-summary.json")
df.createOrReplaceTempView("dfTable")
#df = spark.sql("SELECT DEST_COUNTRY_NAME FROM dfTable LIMIT 2")
#df.show()

df1 = df.select("*", expr("DEST_COUNTRY_NAME AS destination"))

df1.show(2)
df.selectExpr("*", "(DEST_COUNTRY_NAME = ORIGIN_COUNTRY_NAME) AS withinCountry").show(2)
df.selectExpr("avg(count)", "count(distinct(DEST_COUNTRY_NAME))")\
.show()
df.select(expr("DEST_COUNTRY_NAME"),col("DEST_COUNTRY_NAME"),column("DEST_COUNTRY_NAME"))\
    .show(2)



