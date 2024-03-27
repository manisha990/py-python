from pyspark.sql import SparkSession
from pyspark.sql.functions import  *
from pyspark.sql.types import *


#SparkSession
spark = SparkSession.builder \
   .appName("deltatable") \
   .config("spark.jars.packages", "io.delta:delta-core_2.12:1.0.0") \
   .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")\
   .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")\
   .config("spark.sql.streaming.checkpointLocation", "/home/xs445-epssha/Desktop/epsa sharma/newcheck")\
   .getOrCreate()

# Define Kafka source
df = spark.readStream \
   .format("kafka") \
   .option("kafka.bootstrap.servers", "localhost:9092") \
   .option("subscribe", "test002") \
   .option("startingOffsets", "earliest") \
   .load()
df.printSchema()  

json_df = df.selectExpr("cast(value as string) as value")
json_schema =  StructType([
   StructField("Date/Time", StringType(),False),
   StructField("LV_ActivePower",DoubleType(),True),
   StructField("Wind_Speed",DoubleType(),True),
   StructField("Theoretical_Power_Curve", DoubleType(), True),
   StructField("Wind_Direction", DoubleType(), True)
 ])
# Apply Schema to JSON value column and expand the value
from pyspark.sql.functions import from_json

json_expanded_df = json_df.withColumn("value", from_json(json_df["value"], json_schema)).select("value.*")

create_signals_map = {
   "LV_ActivePower": "LV_ActivePower",
   "Wind_Speed": "Wind_Speed",
   "Theoretical_Power_Curve": "Theoretical_Power_Curve",
   "Wind_Direction": "Wind_Direction"
}

df02 = json_expanded_df.withColumn('signal_date',to_timestamp(split(json_expanded_df['Date/Time'],' ').getItem(2)))\
   .withColumn('signal_ts',to_timestamp(split(json_expanded_df['Date/Time'],' ').getItem(3)))\
   .withColumn("create_date",date_format(current_date(), 'dd MM yyyy'))\
   .withColumn("create_ts",date_format(current_timestamp(), 'dd MM yyyy HH:mm:ss'))  \
   .withColumn("signals", struct([json_expanded_df[col].alias(col) for col in create_signals_map]))\


df02.printSchema()

query = df02.writeStream \
   .format("delta") \
   .outputMode("append")\
   .option("mergeSchema", "true")\
   .start('/home/xs445-epssha/Downloads/Python-Test/delta/location')
query.awaitTermination()          
