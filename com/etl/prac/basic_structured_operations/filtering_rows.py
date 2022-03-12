from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr

spark = SparkSession.builder.master('local').getOrCreate()
df = spark.read.format("json").load("D:/Santhu_practice/practice/Spark-The-Definitive-Guide/data/flight-data/json/2010-summary.json")
df = df.withColumn("count2",expr("count").cast("long"))
df.show(3)

df = df.where("count < 2")
df.show(2)

#df2.count()
#df2.write
#df2.collect()
#df2.take(2)
#df2.first()
#df2.head()
#df2.toLocalIterator()


df = df.where(col("count") < 2).where(col("ORIGIN_COUNTRY_NAME") != "Romania")
df.show(2)

df1 = df.where("count < 2").where("ORIGIN_COUNTRY_NAME != 'Romania' ")
df1.show(2)

df2 = df.where("count < 2 and ORIGIN_COUNTRY_NAME != 'Romania'")
df2.show(2)

df_filter = df.where(expr("count < 2") & expr("ORIGIN_COUNTRY_NAME != 'Romania'"))
df_filter.show(2)

df_count = df.select("ORIGIN_COUNTRY_NAME", "DEST_COUNTRY_NAME").distinct().count()
print(df_count)  




