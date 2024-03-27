from pyspark.sql.types import * 
from pyspark.sql import SparkSession 
from pyspark.sql.functions import * 
from delta.tables import * 
spark = SparkSession \
    .builder \
      .config("spark.jars.packages", "io.delta:delta-core_2.12:1.0.0") \
   .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")\
   .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")\
    .getOrCreate()

read_delta = spark.read.format("delta").load("/home/xs445-epssha/Downloads/Python-Test/delta/location") 

distinct_signal = read_delta.groupBy("signal_date").agg(countDistinct("signal_ts").alias("distinct_signal_ts")) 
distinct_signal.show() 

# Add a column in the dataframe of above named as `generation_indicator` and it will have value as 


df= read_delta.withColumn("generation_indicator",when(col("signals.LV_ActivePower")<200,"Low")\
                      .when((col("signals.LV_ActivePower")>=200) & (col("signals.LV_ActivePower")<600),"Medium")\
                    .when((col("signals.LV_ActivePower")>=600) & (col("signals.LV_ActivePower")<1000),"High")\
                    .otherwise("Exceptional"))
 # df.printSchema() 
df.show()

#  # Create dataframe from JSON data 
json_data = [
    {"sig_name": "LV_ActivePower", "sig_mapping_name": "LV ActivePower_average"},
    {"sig_name": "Wind_Speed", "sig_mapping_name": "Wind Speed_average"},
    {"sig_name": "Theoretical_Power_Curve", "sig_mapping_name": "Theoretical_Power_Curve_average"},
    {"sig_name": "Wind_Direction", "sig_mapping_name": "Wind Direction_average"},
    {"sig_name": "Wind_Direction", "sig_mapping_name": "Wind Direction_average"}
]
new_df = spark.createDataFrame([Row(**x) for x in json_data])
new_df.show() 

 
# # perform brodcast join between 4th and 5th
broadcast_df=df.join(broadcast(new_df),df["generation_indicator"] == new_df["sig_mapping_name"],"left_outer")
broadcast_df=broadcast_df.withColumn("generation_indicator",broadcast_df.sig_mapping_name) 
broadcast_df = broadcast_df.drop("sig_name", "sig_mapping_name")

broadcast_df.show()


df02 = read_delta("sqltable",  withcolumn("LV_Active"))

spark.sql("Select * form ")




# # average_per_hour_df =read_df.groupBy(hourly_window).agg(
# #     avg("signals.LV_ActivePower").alias("avg_ActivePower"),
# #     avg("signals.WindSpeed").alias("avg_WindSpeed"),
# #     avg("signals.Theoretical_Power_Curve").alias("avg_TheoreticalPowerCurve"),
# #     avg("signals.WindDirection").alias("avg_WindDirection")
# # )
 
# # Show the DataFrame
# average_per_hour_df.show(truncate=False)