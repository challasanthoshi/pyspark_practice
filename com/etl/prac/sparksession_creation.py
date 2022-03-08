from pyspark.sql import SparkSession

spark = SparkSession.builder.master('local').getOrCreate()
#df = spark.range(1000).toDF("number")
#divisBy2 = df.where("number % 2 = 0")
#print(divisBy2.count())
carrier_data = spark\
    .read\
    .option("inferSchema","true")\
    .option("delimiter",",")\
    .option("header","true")\
    .csv("D:\Santhu_practice\datasets\carriers (2).csv")
carrier_data.show(truncate=False)

print(carrier_data.take(10))








