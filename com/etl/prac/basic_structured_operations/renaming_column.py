from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, expr

spark = SparkSession.builder.master('local').getOrCreate()
df = spark.read.format("json").load("D:/Santhu_practice/practice/Spark-The-Definitive-Guide/data/flight-data/json/2012-summary.json")
df1 = df.withColumn("One", lit(5))
df1.printSchema()
df2 = df1.withColumnRenamed("One", "Number_one")
df2.show(3)
df3 = df2.drop("Number_one")
df3.show(3)

df4 = df.withColumn("This Long Column-Name",expr("ORIGIN_COUNTRY_NAME"))
df4.show(5,truncate=False)

df5 = df4.selectExpr("`This Long Column-Name`", "`This Long Column-Name` as `new column`")
df5.show(2)
df6 = df4.drop("This long Column-Name").show(2)