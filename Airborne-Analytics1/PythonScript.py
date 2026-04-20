#df = spark.read.csv("s3://your_bucket/New_project_data/final_data/itinararies.csv",header=True,sep=',');

#df.write.parquet("s3a://your_bucket/New_project_data/final_data/data.parquet");

import logging

logging.basicConfig(filename='example.log', level=logging.INFO, format='%(asctime)s:%(levelname)s:%(message)s')

logging.info('Starting PySpark job')

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("MyApp").getOrCreate()

#read data directly from S3
df1 = spark.read.parquet("s3://your file path");

# set connection parameters
jdbc_url = "jdbc:mysql://{hostname}:{port}/{database}".format(
    hostname="your-rds-instance-hostname",
    port=3306,
    database="your-database-name"
)
connection_properties = {
    "user": "your-username",
    "password": "your-password"
}

# read a MySQL table into a PySpark dataframe
table_name = "your-table-name"
df2 = spark.read.jdbc(url=jdbc_url, table=table_name, properties=connection_properties)


dp = df1.unionByName(df2)

dp = dp.drop('segmentsCabinCode','segmentsArrivalTimeEpochSeconds','segmentsDepartureTimeEpochSeconds','totalFare','elapsedDays','fareBasisCode','searchDate')

#dp = dp.dropna(subset=['segmentsEquipmentDescription', 'totalTravelDistance'])

from pyspark.sql.functions import *;

dp = dp.withColumn('Arr1',split(dp['segmentsArrivalAirportCode'],'\|\|').getItem(0));

dp = dp.withColumn('Arr2',split(dp['segmentsArrivalAirportCode'],'\|\|').getItem(1));

dp = dp.withColumn('Arr3',split(dp['segmentsArrivalAirportCode'],'\|\|').getItem(2));

dp = dp.drop('segmentsArrivalAirportCode');

dp = dp.withColumn('Depart1',split(dp['segmentsDepartureAirportCode'],'\|\|').getItem(0));

dp = dp.withColumn('Depart2',split(dp['segmentsDepartureAirportCode'],'\|\|').getItem(1));

dp = dp.withColumn('Depart3',split(dp['segmentsDepartureAirportCode'],'\|\|').getItem(2));

dp = dp.drop('segmentsDepartureAirportCode');

dp = dp.withColumn('Airline1',split(dp['segmentsAirlineName'],'\|\|').getItem(0));

dp = dp.withColumn('Airline2',split(dp['segmentsAirlineName'],'\|\|').getItem(1));

dp = dp.withColumn('Airline3',split(dp['segmentsAirlineName'],'\|\|').getItem(2));

dp = dp.drop('segmentsAirlineName');

dp = dp.withColumn('AirCode1',split(dp['segmentsAirlineCode'],'\|\|').getItem(0));

dp = dp.withColumn('AirCode2',split(dp['segmentsAirlineCode'],'\|\|').getItem(1));

dp = dp.withColumn('AirCode3',split(dp['segmentsAirlineCode'],'\|\|').getItem(2));

dp = dp.drop('segmentsAirlineCode');

dp = dp.withColumn('Aircraft1',split(dp['segmentsEquipmentDescription'],'\|\|').getItem(0));

dp = dp.withColumn('Aircraft2',split(dp['segmentsEquipmentDescription'],'\|\|').getItem(1));

dp = dp.withColumn('Aircraft3',split(dp['segmentsEquipmentDescription'],'\|\|').getItem(2));

dp = dp.drop('segmentsEquipmentDescription');

dp = dp.withColumn('dist1',split(dp['segmentsDistance'],'\|\|').getItem(0));

dp = dp.withColumn('dist2',split(dp['segmentsDistance'],'\|\|').getItem(1));

dp = dp.withColumn('dist3',split(dp['segmentsDistance'],'\|\|').getItem(2));

dp = dp.drop('segmentsDistance');

dp = dp.withColumn('durSec1',split(dp['segmentsDurationInSeconds'],'\|\|').getItem(0));

dp = dp.withColumn('durSec2',split(dp['segmentsDurationInSeconds'],'\|\|').getItem(1));

dp = dp.withColumn('durSec3',split(dp['segmentsDurationInSeconds'],'\|\|').getItem(2));

dp = dp.drop('segmentsDurationInSeconds');

dp = dp.withColumn('AT1',split(dp['segmentsDepartureTimeRaw'], '\|\|').getItem(0))
dp = dp.withColumn('AT2',split(dp['segmentsDepartureTimeRaw'], '\|\|').getItem(1))
dp = dp.withColumn ('AT3',split(dp['segmentsDepartureTimeRaw'], '\|\|').getItem(2))
dp = dp.drop('partition')
dp = dp.withColumn('Departure_Date1', split(dp ['AT1'], 'T').getItem(0))
dp = dp.withColumn('Time1', split(dp ['AT1'], 'T').getItem(1))
dp = dp.withColumn('Departure_Date2', split(dp ['AT2'], 'T').getItem(0)) 
dp = dp.withColumn('Time2', split(dp ['AT2'], 'T').getItem(1))
dp = dp.withColumn('Departure_Date3', split(dp ['AT3'], 'T').getItem(0))
dp = dp.withColumn('Time3', split(dp ['AT3'], 'T').getItem(1))

dp = dp.drop('AT1', 'AT2','AT3')

dp = dp.withColumn('T1', split(dp ['Time1'], '-').getItem(0))
dp = dp.withColumn('offset1', split(dp ['Time1'], '-').getItem(1))
dp = dp.withColumn("Time_wo_seconds", substring_index(dp ["T1"], ":", 2))
dp = dp.withColumn("time1_unix", unix_timestamp(dp ["Time_wo_seconds"], "HH:mm"))
dp = dp.withColumn("time2_unix", unix_timestamp(dp ["offset1"], "HH:mm"))
dp = dp.withColumn("time_diff_unix", (dp ["time1_unix"] - dp ["time2_unix"]))
dp = dp.withColumn("Departure_local_time1", from_unixtime(dp["time_diff_unix"], "HH:mm"))
dp = dp.drop("time1_unix", "time2_unix", "time_diff_unix",'T1', 'offset1', "Time_wo_seconds")


dp = dp.withColumn('T2', split(dp ['Time2'], '-').getItem(0))
dp = dp.withColumn('offset2', split(dp ['Time2'], '-').getItem(1))
dp = dp.withColumn("Time_wo_seconds", substring_index(dp ["T2"], ":", 2))
dp = dp.withColumn("time1_unix", unix_timestamp(dp ["Time_wo_seconds"], "HH:mm"))
dp = dp.withColumn("time2_unix", unix_timestamp(dp ["offset2"], "HH:mm"))
dp = dp.withColumn("time_diff_unix", (dp["time1_unix"] - dp["time2_unix"]))
dp = dp.withColumn("Departure_local_time2", from_unixtime(dp ["time_diff_unix"], "HH:mm"))
dp = dp.drop("time1_unix", "time2_unix", "time_diff_unix","T2","offset2","Time_wo_seconds")

dp = dp.withColumn('T3', split(dp ['Time3'], '-').getItem(0))
dp = dp.withColumn('offset3', split(dp ['Time3'], '-').getItem(1))
dp = dp.withColumn("Time_wo_seconds", substring_index(dp ["T3"], ":", 2))
dp = dp.withColumn("time1_unix", unix_timestamp(dp ["Time_wo_seconds"], "HH:mm"))
dp = dp.withColumn("time2_unix", unix_timestamp(dp ["offset3"], "HH:mm"))
dp = dp.withColumn("time_diff_unix", (dp ["time1_unix"] - dp["time2_unix"]))
dp = dp.withColumn("Departure_local_time3", from_unixtime(dp ["time_diff_unix"], "HH:mm"))
dp = dp.drop("time1_unix", "time2_unix", "time_diff_unix","T3","offset3","Time_wo_seconds")

dp = dp.drop("Time1","Time2","Time3")

dp = dp.withColumn('AT1',split(dp['segmentsArrivalTimeRaw'], '\|\|').getItem(0))
dp = dp.withColumn('AT2',split(dp['segmentsArrivalTimeRaw'], '\|\|').getItem(1))
dp = dp.withColumn ('AT3',split(dp['segmentsArrivalTimeRaw'], '\|\|').getItem(2))

dp = dp.withColumn('Arrival_Date1', split(dp ['AT1'], 'T').getItem(0))
dp = dp.withColumn('Time1', split(dp ['AT1'], 'T').getItem(1))
dp = dp.withColumn('Arrival_Date2', split(dp ['AT2'], 'T').getItem(0))
dp = dp.withColumn('Time2', split(dp ['AT2'], 'T').getItem(1))
dp = dp.withColumn('Arrival_Date3', split(dp ['AT3'], 'T').getItem(0))
dp = dp.withColumn('Time3', split(dp ['AT3'], 'T').getItem(1))

dp = dp.drop('AT1', 'AT2','AT3')

dp = dp.withColumn('T1', split(dp ['Time1'], '-').getItem(0))
dp = dp.withColumn('offset1', split(dp ['Time1'], '-').getItem(1))
dp = dp.withColumn("Time_wo_seconds", substring_index(dp ["T1"], ":", 2))
dp = dp.withColumn("time1_unix", unix_timestamp(dp ["Time_wo_seconds"], "HH:mm"))
dp = dp.withColumn("time2_unix", unix_timestamp(dp ["offset1"], "HH:mm"))
dp = dp.withColumn("time_diff_unix", (dp ["time1_unix"] - dp ["time2_unix"]))
dp = dp.withColumn("Arrival_local_time1", from_unixtime(dp["time_diff_unix"], "HH:mm"))
dp = dp.drop("time1_unix", "time2_unix", "time_diff_unix",'T1', 'offset1', "Time_wo_seconds")


dp = dp.withColumn('T2', split(dp ['Time2'], '-').getItem(0))
dp = dp.withColumn('offset2', split(dp ['Time2'], '-').getItem(1))
dp = dp.withColumn("Time_wo_seconds", substring_index(dp ["T2"], ":", 2))
dp = dp.withColumn("time1_unix", unix_timestamp(dp ["Time_wo_seconds"], "HH:mm"))
dp = dp.withColumn("time2_unix", unix_timestamp(dp ["offset2"], "HH:mm"))
dp = dp.withColumn("time_diff_unix", (dp["time1_unix"] - dp["time2_unix"]))
dp = dp.withColumn("Arrival_local_time2", from_unixtime(dp ["time_diff_unix"], "HH:mm"))
dp = dp.drop("time1_unix", "time2_unix", "time_diff_unix","T2","offset2","Time_wo_seconds")

dp = dp.withColumn('T3', split(dp ['Time3'], '-').getItem(0))
dp = dp.withColumn('offset3', split(dp ['Time3'], '-').getItem(1))
dp = dp.withColumn("Time_wo_seconds", substring_index(dp ["T3"], ":", 2))
dp = dp.withColumn("time1_unix", unix_timestamp(dp ["Time_wo_seconds"], "HH:mm"))
dp = dp.withColumn("time2_unix", unix_timestamp(dp ["offset3"], "HH:mm"))
dp = dp.withColumn("time_diff_unix", (dp["time1_unix"] - dp["time2_unix"]))
dp = dp.withColumn("Arrival_local_time3", from_unixtime(dp ["time_diff_unix"], "HH:mm"))
dp = dp.drop("time1_unix", "time2_unix", "time_diff_unix","T3","offset3","Time_wo_seconds")
dp = dp.drop("Time1","Time2","Time3")
dp = dp.drop('segmentsDepartureTimeRaw','segmentsArrivalTimeRaw')

dp = dp.withColumn('Travel_Duration', substring('travelDuration', 3, 8))

dp = dp.drop('Stime','travelDuration')

dp.printSchema()

dp.write.parquet("s3://your output bucket");

logging.info('PySpark job completed')
