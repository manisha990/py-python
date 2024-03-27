from pyspark.sql import SparkSession
from pyspark.sql import Row

# Create SparkSession
spark = SparkSession.builder \
    .appName("CreateDataFrameFromJSON") \
    .getOrCreate()

# JSON data
json_data = [
    {"sig_name": "LV_ActivePower", "sig_mapping_name": "LV ActivePower_average"},
    {"sig_name": "Wind_Speed", "sig_mapping_name": "Wind Speed_average"},
    {"sig_name": "Theoretical_Power_Curve", "sig_mapping_name": "Theoretical_Power_Curve_average"},
    {"sig_name": "Wind_Direction", "sig_mapping_name": "Wind Direction_average"},
    {"sig_name": "Wind_Direction", "sig_mapping_name": "Wind Direction_average"}
]

# Create DataFrame from JSON data
df = spark.createDataFrame([Row(**x) for x in json_data])

# Show the DataFrame
df.show()

     