from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql.functions import broadcast

# Create SparkSession
spark = SparkSession.builder \
    .appName("ChangeSignalNames") \
    .getOrCreate()

# JSON data for signal mapping

df= read_df.withColumn("generation_indicator",when(col("signals.LV_ActivePower")<200,"Low")\
                      .when((col("signals.LV_ActivePower")>=200) & (col("signals.LV_ActivePower")<600),"Medium")\
                    .when((col("signals.LV_ActivePower")>=600) & (col("signals.LV_ActivePower")<1000),"High")\
                    .otherwise("Exceptional"))
