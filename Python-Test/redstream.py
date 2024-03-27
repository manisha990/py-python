from pyspark.sql import SparkSession
from pyspark.sql.functions import expr
from pyspark.sql.types import *
from pyspark.sql.functions import from_json

spark = SparkSession.builder.appName("CSV to Kafka").config("spark.jars.packages", "io.delta:delta-core_2.12:1.0.0") \
   .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")\
    .getOrCreate()
schema = StructType([
    StructField("Date/Time",StringType(),True),
   StructField("LV_ActivePower",DoubleType(),True),
   StructField("Wind_Speed",DoubleType(),True),
   StructField("Theoretical_Power_Curve", DoubleType(), True),
   StructField("Wind_Direction", DoubleType(), True)
])

df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "test002") \
    .option("startingOffsets", "earliest") \
    .load()
df.printSchema()

json_df = df.selectExpr("cast(value as string) as value")
json_expanded_df = json_df.withColumn("value", from_json(json_df["value"], schema)).select("value.*")


query = json_expanded_df.writeStream \
   .outputMode("append") \
   .format("console") \
   .start()\
   .awaitTermination()  