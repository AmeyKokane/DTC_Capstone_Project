#!/usr/bin/env python
# coding: utf-8

import argparse

import pyspark
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number

parser = argparse.ArgumentParser()

parser.add_argument('--input_crashes', required=True)
parser.add_argument('--input_vehicles', required=True)
parser.add_argument('--input_people', required=True)
parser.add_argument('--output', required=True)

args = parser.parse_args()

input_crashes = args.input_crashes
input_vehicles = args.input_vehicles
input_people = args.input_people
output = args.output


spark = SparkSession.builder \
    .appName('test') \
    .getOrCreate()

spark.conf.set('temporaryGcsBucket', 'dataproc-temp-us-central1-113206216777-gcnmgobb')

#Bringing in only necessary fields for Crashes dataset
crashes_columns = ['CRASH_RECORD_ID','RD_NO','CRASH_DATE','POSTED_SPEED_LIMIT','WEATHER_CONDITION','LIGHTING_CONDITION','FIRST_CRASH_TYPE','STREET_NO','STREET_DIRECTION','STREET_NAME','INJURIES_TOTAL','INJURIES_FATAL','prim_contributory_cause']
df_crashes = spark.read.parquet(input_crashes).select(crashes_columns)

#Bringing in only necessary fields for Vehicles dataset
vehicles_columns = ['CRASH_UNIT_ID','CRASH_RECORD_ID','RD_NO','CRASH_DATE','UNIT_NO','UNIT_TYPE','NUM_PASSENGERS','VEHICLE_ID','CMRC_VEH_I','MAKE','MODEL']
df_vehicles = spark.read.parquet(input_vehicles).select(vehicles_columns)

#Bringing in only necessary fields for People dataset
people_columns = ['PERSON_ID','CRASH_RECORD_ID','RD_NO','CRASH_DATE','PERSON_TYPE','SEX','AGE','INJURY_CLASSIFICATION','VEHICLE_ID']
df_people = spark.read.parquet(input_people).select(people_columns)

#Adding sequence number for each vehicle involved in a crash in Vehicles Dataset
windowSpec  = Window.partitionBy("CRASH_RECORD_ID").orderBy("CRASH_UNIT_ID")
df_vehicles= df_vehicles.withColumn("veh_seq_nbr",row_number().over(windowSpec))

# Creating Temp tables for Vehicles, Crashes & People datasets for further transformations that will be done using Spark SQL
df_vehicles.createOrReplaceTempView('vehicles')
df_crashes.createOrReplaceTempView('crashes')
df_people.createOrReplaceTempView('people')



# Summarizing vehicles data at crash_record_id level so it can be joined onto crashes dataset to create a final dataset to be exported back to GCP
df_summary_table = spark.sql("""
select 
a.CRASH_RECORD_ID, 
a.CRASH_DATE, 
a.CRASH_UNIT_ID,
a.VEHICLE_ID,
a.MAKE,
case when a.MAKE= 'OTHER (EXPLAIN IN NARRATIVE)' THEN 'N/A'
     when a.MAKE= 'UNKNOWN' THEN 'N/A' 
     when a.MAKE= ' ' THEN 'N/A'
     else a.MAKE end as Vehicle_Make,
a.MODEL,
case when a.MODEL= 'OTHER (EXPLAIN IN NARRATIVE)' THEN 'N/A'
     when a.MODEL= 'UNKNOWN' THEN 'N/A' 
     when a.MODEL= ' ' THEN 'N/A'
     else a.MODEL end as Vehicle_Model,
a.UNIT_TYPE,
b.PERSON_ID,
b.PERSON_TYPE,
b.SEX,
b.AGE,
c.weather_condition, 
c.lighting_condition,
c.prim_contributory_cause, 
c.street_no||' '||c.street_direction||' '||c.street_name as street,
c.INJURIES_TOTAL,
c.INJURIES_FATAL

from vehicles as a
left join people as b
on a.CRASH_RECORD_ID=b.CRASH_RECORD_ID
and a.CRASH_UNIT_ID=b.VEHICLE_ID
and a.CRASH_DATE=b.CRASH_DATE

LEFT JOIN crashes as c
on a.CRASH_RECORD_ID=c.CRASH_RECORD_ID
and a.CRASH_DATE=c.CRASH_DATE
    
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19
""")

df_summary_table.coalesce(1) \
.write.parquet(output, mode='overwrite')