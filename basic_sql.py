from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("demo").getOrCreate()
df = spark.createDataFrame(
   [
       ("sue", 32),
       ("li", 3),
       ("bob", 75),
       ("heo", 13),
   ],
   ["first_name", "age"],
)
spark.sql("DROP TABLE IF EXISTS some_people")
df.write.saveAsTable("some_people")
spark.sql("select * from some_people").show()
