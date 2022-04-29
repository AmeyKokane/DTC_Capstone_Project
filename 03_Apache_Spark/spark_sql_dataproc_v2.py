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
crashes_columns = ['CRASH_RECORD_ID','RD_NO','CRASH_DATE','POSTED_SPEED_LIMIT','WEATHER_CONDITION','LIGHTING_CONDITION','FIRST_CRASH_TYPE','STREET_NO','STREET_DIRECTION','STREET_NAME','INJURIES_TOTAL','INJURIES_FATAL']
df_crashes = spark.read.parquet(input_crashes).select(crashes_columns)

#Bringing in only necessary fields for Vehicles dataset
vehicles_columns = ['CRASH_UNIT_ID','CRASH_RECORD_ID','RD_NO','CRASH_DATE','UNIT_NO','UNIT_TYPE','NUM_PASSENGERS','VEHICLE_ID','CMRC_VEH_I','MAKE','MODEL']
df_vehicles = spark.read.parquet(input_vehicles).select(vehicles_columns)

#Bringing in only necessary fields for People dataset
people_columns = ['PERSON_ID','CRASH_RECORD_ID','RD_NO','CRASH_DATE','PERSON_TYPE','SEX','AGE','INJURY_CLASSIFICATION']
df_people = spark.read.parquet(input_people).select(people_columns)

#Adding sequence number for each vehicle involved in a crash in Vehicles Dataset
windowSpec  = Window.partitionBy("CRASH_RECORD_ID").orderBy("CRASH_UNIT_ID")
df_vehicles= df_vehicles.withColumn("veh_seq_nbr",row_number().over(windowSpec))

# Creating Temp tables for Vehicles, Crashes & People datasets for further transformations that will be done using Spark SQL
df_vehicles.createOrReplaceTempView('vehicles')
df_crashes.createOrReplaceTempView('crashes')
df_people.createOrReplaceTempView('people')

# Summarizing vehicles data at crash_record_id level so it can be joined onto crashes dataset to create a final dataset to be exported back to GCP
df_vehicle_summ = spark.sql("""
SELECT B.CRASH_RECORD_ID,
    B.RD_NO,
    B.CRASH_DATE, 
    B.Cnt_Entities_Accid,
    B.Cnt_MotorVehicles_Accid,
    
    max(VEHICLE1_ID) as VEHICLE1_ID,
    max(VEHICLE1_MAKE) as VEHICLE1_MAKE,
    max(VEHICLE1_MODEL) as VEHICLE1_MODEL,
    
    max(VEHICLE2_ID) as VEHICLE2_ID,
    max(VEHICLE2_MAKE) as VEHICLE2_MAKE,
    max(VEHICLE2_MODEL) as VEHICLE2_MODEL,
    
    max(VEHICLE3_ID) as VEHICLE3_ID ,
    max(VEHICLE3_MAKE) as VEHICLE3_MAKE,
    max(VEHICLE3_MODEL) as VEHICLE3_MODEL ,
    
    max(VEHICLE4_ID) as VEHICLE4_ID ,
    max(VEHICLE4_MAKE) as VEHICLE4_MAKE,
    max(VEHICLE4_MODEL) as VEHICLE4_MODEL
    FROM 
(SELECT 
    a.CRASH_RECORD_ID,
    a.RD_NO,
    a.CRASH_DATE, 
    
    b1.CRASH_UNIT_ID as VEHICLE1_ID,
    case when b1.MAKE is null then b1.UNIT_TYPE else b1.MAKE end as VEHICLE1_MAKE,
    case when b1.MODEL is null then b1.UNIT_TYPE else b1.MODEL end as VEHICLE1_MODEL,
    
    b2.CRASH_UNIT_ID as VEHICLE2_ID,
    case when b2.MAKE is null then b2.UNIT_TYPE else b2.MAKE end as VEHICLE2_MAKE,
    case when b2.MODEL is null then b2.UNIT_TYPE else b2.MODEL end as VEHICLE2_MODEL,
    
    b3.CRASH_UNIT_ID as VEHICLE3_ID,
    case when b3.MAKE is null then b3.UNIT_TYPE else b3.MAKE end as VEHICLE3_MAKE,
    case when b3.MODEL is null then b3.UNIT_TYPE else b3.MODEL end as VEHICLE3_MODEL,
    
    b4.CRASH_UNIT_ID as VEHICLE4_ID,
    case when b4.MAKE is null then b4.UNIT_TYPE else b4.MAKE end as VEHICLE4_MAKE,
    case when b4.MODEL is null then b4.UNIT_TYPE else b4.MODEL end as VEHICLE4_MODEL,
    
    COUNT(a.CRASH_UNIT_ID) as Cnt_Entities_Accid,
    COUNT(a.VEHICLE_ID) as Cnt_MotorVehicles_Accid
    
FROM
    vehicles a
    
    left join (select CRASH_RECORD_ID, CRASH_DATE, CRASH_UNIT_ID,VEHICLE_ID,MAKE,MODEL,UNIT_TYPE
                from vehicles where veh_seq_nbr = 1 ) b1
    on a.CRASH_RECORD_ID = b1.CRASH_RECORD_ID
    and a.CRASH_DATE = b1.CRASH_DATE
    and a.CRASH_UNIT_ID = b1.CRASH_UNIT_ID
    and a.VEHICLE_ID = b1.VEHICLE_ID
    
    
    left join (select CRASH_RECORD_ID, CRASH_DATE, CRASH_UNIT_ID,VEHICLE_ID,MAKE,MODEL,UNIT_TYPE
                from vehicles where  veh_seq_nbr = 2) b2
    on a.CRASH_RECORD_ID = b2.CRASH_RECORD_ID
    and a.CRASH_DATE = b2.CRASH_DATE
    and a.CRASH_UNIT_ID = b2.CRASH_UNIT_ID
    and a.VEHICLE_ID = b2.VEHICLE_ID
    
    left join (select CRASH_RECORD_ID, CRASH_DATE, CRASH_UNIT_ID,VEHICLE_ID,MAKE,MODEL,UNIT_TYPE
                from vehicles where  veh_seq_nbr = 3) b3
    on a.CRASH_RECORD_ID = b3.CRASH_RECORD_ID
    and a.CRASH_DATE = b3.CRASH_DATE
    and a.CRASH_UNIT_ID = b3.CRASH_UNIT_ID
    and a.VEHICLE_ID = b3.VEHICLE_ID
    
    left join (select CRASH_RECORD_ID, CRASH_DATE, CRASH_UNIT_ID,VEHICLE_ID,MAKE,MODEL ,UNIT_TYPE
                from vehicles where  veh_seq_nbr = 4) b4
    on a.CRASH_RECORD_ID = b4.CRASH_RECORD_ID
    and a.CRASH_DATE = b4.CRASH_DATE
    and a.CRASH_UNIT_ID = b4.CRASH_UNIT_ID
    and a.VEHICLE_ID = b4.VEHICLE_ID
    
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15) B
group by 1,2,3,4,5
""")


#Creating Temp table for Vehicles Summary df created above. This temp will be used to join Vehicles Summary temp table to Crashes temp table.
df_vehicle_summ.createOrReplaceTempView('vehicles_summ')

# Summarizing people data at crash_record_id level so it can be joined onto crashes dataset to create a final dataset to be exported back to GCP
df_people_summ = spark.sql("""
SELECT B.CRASH_RECORD_ID,
    B.CRASH_DATE, 
    B.RD_NO, 
    B.Cnt_Drivers_Accid,
    max(PERSON1_ID) as PERSON1_ID,
    max(PERSON1_SEX) as PERSON1_SEX,
    max(PERSON1_AGE) as PERSON1_AGE,
    max(PERSON1_INJURY_CLASSIFICATION) as PERSON1_INJURY_CLASSIFICATION,
    
    max(PERSON2_ID) as PERSON2_ID,
    max(PERSON2_SEX) as PERSON2_SEX,
    max(PERSON2_AGE) as PERSON2_AGE,
    max(PERSON2_INJURY_CLASSIFICATION) as PERSON2_INJURY_CLASSIFICATION,
    
    max(PERSON3_ID) as PERSON3_ID ,
    max(PERSON3_SEX) as PERSON3_SEX,
    max(PERSON3_AGE) as PERSON3_AGE,
    max(PERSON3_INJURY_CLASSIFICATION) as PERSON3_INJURY_CLASSIFICATION,

    max(PERSON4_ID) as PERSON4_ID ,
    max(PERSON4_SEX) as PERSON4_SEX,
    max(PERSON4_AGE) as PERSON4_AGE,
    max(PERSON4_INJURY_CLASSIFICATION) as PERSON4_INJURY_CLASSIFICATION

    FROM 
(SELECT 
    a.CRASH_RECORD_ID,
    a.RD_NO,
    a.CRASH_DATE, 
    
    b1.PERSON_ID as PERSON1_ID,
    case when b1.SEX is null then 'NA' else b1.SEX end as PERSON1_SEX,
    case when b1.AGE is null then 'NA' else b1.AGE end as PERSON1_AGE,
    case when b1.INJURY_CLASSIFICATION is null then 'NA' else b1.AGE end as PERSON1_INJURY_CLASSIFICATION,
    
    b2.PERSON_ID as PERSON2_ID,
    case when b2.SEX is null then 'NA' else b2.SEX end as PERSON2_SEX,
    case when b2.AGE is null then 'NA' else b2.AGE end as PERSON2_AGE,
    case when b2.INJURY_CLASSIFICATION is null then 'NA' else b2.INJURY_CLASSIFICATION end as PERSON2_INJURY_CLASSIFICATION,
    
    b3.PERSON_ID as PERSON3_ID,
    case when b3.SEX is null then 'NA' else b3.SEX end as PERSON3_SEX,
    case when b3.AGE is null then 'NA' else b3.AGE end as PERSON3_AGE,
    case when b3.INJURY_CLASSIFICATION is null then 'NA' else b3.INJURY_CLASSIFICATION end as PERSON3_INJURY_CLASSIFICATION,
    
    b4.PERSON_ID as PERSON4_ID,
    case when b4.SEX is null then 'NA' else b4.SEX end as PERSON4_SEX,
    case when b4.AGE is null then 'NA' else b4.AGE end as PERSON4_AGE,
    case when b4.INJURY_CLASSIFICATION is null then 'NA' else b4.INJURY_CLASSIFICATION end as PERSON4_INJURY_CLASSIFICATION,
    
    count(a.person_id) as Cnt_Drivers_Accid
FROM
    people a

    left join (select CRASH_RECORD_ID, RD_NO, CRASH_DATE, PERSON_ID,PERSON_TYPE,SEX,AGE,INJURY_CLASSIFICATION
                from people where PERSON_TYPE = 'Driver') b1
    on a.CRASH_RECORD_ID = b1.CRASH_RECORD_ID
    and a.CRASH_DATE = b1.CRASH_DATE
    and a.PERSON_ID  = b1.PERSON_ID    
    
    left join (select CRASH_RECORD_ID, CRASH_DATE, PERSON_ID,PERSON_TYPE,SEX,AGE,INJURY_CLASSIFICATION
                from people where PERSON_TYPE = 'Driver') b2
    on a.CRASH_RECORD_ID = b2.CRASH_RECORD_ID
    and a.CRASH_DATE = b2.CRASH_DATE
    and a.PERSON_ID  = b2.PERSON_ID 
    
    left join (select CRASH_RECORD_ID, CRASH_DATE, PERSON_ID,PERSON_TYPE,SEX,AGE,INJURY_CLASSIFICATION
                from people where PERSON_TYPE = 'Driver') b3
    on a.CRASH_RECORD_ID = b3.CRASH_RECORD_ID
    and a.CRASH_DATE = b3.CRASH_DATE
    and a.PERSON_ID  = b3.PERSON_ID 
    
    left join (select CRASH_RECORD_ID, CRASH_DATE, PERSON_ID,PERSON_TYPE,SEX,AGE,INJURY_CLASSIFICATION
                from people where PERSON_TYPE = 'Driver') b4
    on a.CRASH_RECORD_ID = b4.CRASH_RECORD_ID
    and a.CRASH_DATE = b4.CRASH_DATE
    and a.PERSON_ID  = b4.PERSON_ID 
where a.PERSON_TYPE = 'Driver'   
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19) B
group by 1,2,3,4
""")

#Creating Temp table for People Summary df created above. This temp will be used to join Vehicles Summary temp table & Crashes temp table.
df_people_summ.createOrReplaceTempView('people_summ')

# Summarizing vehicles data at crash_record_id level so it can be joined onto crashes dataset to create a final dataset to be exported back to GCP
df_final_summ = spark.sql("""
SELECT 
    A.CRASH_RECORD_ID,
    A.RD_NO,
    A.CRASH_DATE,
    date_trunc('month', A.CRASH_DATE) AS CRASH_MONTH,
    date_trunc('year', A.CRASH_DATE) AS CRASH_YEAR,
    date_trunc('year', A.CRASH_DATE)||'_'||date_trunc('month', A.CRASH_DATE) AS YEAR_MONTH,
    CASE WHEN EXTRACT(DOW FROM A.CRASH_DATE) = 0 THEN 'SUNDAY'
         WHEN EXTRACT(DOW FROM A.CRASH_DATE) = 1 THEN 'MONDAY'
         WHEN EXTRACT(DOW FROM A.CRASH_DATE) = 2 THEN 'TUESDAY'
         WHEN EXTRACT(DOW FROM A.CRASH_DATE) = 3 THEN 'WEDNESDAY'
         WHEN EXTRACT(DOW FROM A.CRASH_DATE) = 4 THEN 'THURSDAY'
         WHEN EXTRACT(DOW FROM A.CRASH_DATE) = 5 THEN 'FRIDAY'
         WHEN EXTRACT(DOW FROM A.CRASH_DATE) = 6 THEN 'SATURDAY' 
         END AS DAY_OF_WEEK,
    EXTRACT(HOUR FROM A.CRASH_DATE) AS HOUR_OF_DAY,
    A.POSTED_SPEED_LIMIT,
    A.WEATHER_CONDITION,
    A.LIGHTING_CONDITION,
    A.FIRST_CRASH_TYPE,
    A.STREET_NO,
    A.STREET_DIRECTION,
    A.STREET_NAME,
    A.INJURIES_TOTAL,
    A.INJURIES_FATAL,
    B.CNT_ENTITIES_ACCID,
    B.Cnt_MOTORVEHICLES_ACCID,
    B.VEHICLE1_ID,
    B.VEHICLE1_MAKE,
    B.VEHICLE1_MODEL,
    B.VEHICLE2_ID,
    B.VEHICLE2_MAKE,
    B.VEHICLE2_MODEL,
    B.VEHICLE3_ID ,
    B.VEHICLE3_MAKE,
    B.VEHICLE3_MODEL ,
    B.VEHICLE4_ID ,
    B.VEHICLE4_MAKE,
    B.VEHICLE4_MODEL,
    C.Cnt_Drivers_Accid,
    C.PERSON1_ID,
    C.PERSON1_SEX,
    C.PERSON1_AGE,
    C.PERSON1_INJURY_CLASSIFICATION,
    C.PERSON2_ID,
    C.PERSON2_SEX,
    C.PERSON2_AGE,
    C.PERSON2_INJURY_CLASSIFICATION,
    C.PERSON3_ID ,
    C.PERSON3_SEX,
    C.PERSON3_AGE,
    C.PERSON3_INJURY_CLASSIFICATION,
    C.PERSON4_ID ,
    C.PERSON4_SEX,
    C.PERSON4_AGE,
    C.PERSON4_INJURY_CLASSIFICATION

FROM crashes A

LEFT JOIN vehicles_summ B
    ON A.CRASH_RECORD_ID = B.CRASH_RECORD_ID
    and A.CRASH_DATE = B.CRASH_DATE
    and A.RD_NO = B.RD_NO

LEFT JOIN people_summ C
    ON A.CRASH_RECORD_ID = C.CRASH_RECORD_ID
    and A.CRASH_DATE = C.CRASH_DATE
    and A.RD_NO = C.RD_NO

group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,39,40,41,42,43,44,45,46,47,48

""")

df_final_summ.coalesce(1) \
    .write.parquet(output, mode='overwrite')