from pyspark.sql import SparkSession, functions

spark = SparkSession.builder.master('local').getOrCreate()
df = spark.sql("select 'santhu' as name")
df.show()

df = df.withColumn("id", functions.lit("10000"))
df.show()