from pyspark.sql import SparkSession

spark = SparkSession.builder.master('local').getOrCreate()

df = spark.range(500).toDF("number")
df.select(df["number"] + 10).show(10
                                  )
