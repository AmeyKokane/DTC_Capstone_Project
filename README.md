# DataTalks Club DE Zoomcamp Capstone Project by Amey Kokane

This repository contains my code for DataTalkClub's DE Zoomcamp Capstone Project. Final dashboard is available [here.](https://datastudio.google.com/s/m5F6ay7Yp0k) If you have stumbled upon this repo in future and are not able to access dashboard link, it means my Google Cloud free credits have expired. I have included a screen shot of the dashboard below so please scroll down if interested in dashboard.

Kudos to DataTalks Club Team for putting this [DE Zoomcamp](https://github.com/DataTalksClub/data-engineering-zoomcamp) together!! 

## Objective:
Commute in Chicago during office hours is painfully slow. Most of the times traffic slows down due to a crash on a roadway. I have always wondered how many crashes happen in around Chicagoland. City of Chicago's open data portal lets user download datasets for personal analysis.
Hence I wanted to create a data pipeline to extract data from various Chicago traffic crashes related datasets, transform those datasets into a single stable dataset that can be loaded into a data warehouse on a certain cadence to be used by data scientists/analysts is the goal of this exercise.

## Data Sources:
1. Traffic Crashes - [Crashes](https://data.cityofchicago.org/Transportation/Traffic-Crashes-Crashes/85ca-t3if)
  Crash data shows information about each traffic crash on city streets within the City of Chicago limits and under the jurisdiction
  of Chicago Police Department (CPD). Data are shown as is from the electronic crash reporting system (E-Crash) at CPD, excluding any  
  personally identifiable information.
  
2. Traffic Crashes - [Vehicles](https://data.cityofchicago.org/Transportation/Traffic-Crashes-Vehicles/68nd-jvt3)
  This dataset contains information about vehicles (or units as they are identified in crash reports) involved in a traffic crash.
  
3. Traffic Crashes - [People](https://data.cityofchicago.org/Transportation/Traffic-Crashes-People/u6pd-qa9d)
  This data contains information about people involved in a crash and if any injuries were sustained.

## Tools & Tech Stack Used:
1. Infrastructure as Code --> [Terraform](https://www.terraform.io)
2. Cloud Platform --> [Google Cloud](https://cloud.google.com)
3. Data Lake --> [Google Cloud Storage](https://cloud.google.com/storage/)
4. Data Warehouse --> [Google BigQuery](https://cloud.google.com/bigquery)
5. Data Transformation:
  a.Pre Load --> [Python Pandas Library](https://pandas.pydata.org) and [Python Pyarrow Library](https://arrow.apache.org/docs/python/index.html)
  b.Post Load Batch Processing --> [Apache Spark](https://spark.apache.org) and [Google DataProc](https://cloud.google.com/dataproc)
6. Workflow Orchestration --> [Airflow](https://airflow.apache.org)
7. Containerization --> [Docker](https://www.docker.com) and [Docker Compose](https://docs.docker.com/compose/)
8. Data Vizualization Tool --> [Google Data Studio](https://datastudio.google.com/)

## Data Pipeline Architecture:
![alt text](https://github.com/AmeyKokane/DTC_Capstone_Project/blob/main/00_Images/data_pipeline_workflow.jpg "Data Pipeline Architecture")


## Final Analytical Dashboard:
![alt text](https://github.com/AmeyKokane/DTC_Capstone_Project/blob/main/00_Images/Dashboard_fin_img.png "Dashboard")


## Step-by-Step Guide:
1. Provision Cloud Infrastructure
  a. Create Google Cloud Platform Account
  b. Create new project 
  c. Configure Identity and Access Management (IAM) for service account. You will need to assign this account BigQuery Admin, Storage
  Admin, Storage Object Admin, Viewer, DataProc Admin, DataProc Service Agent previliges.
  d. Download the JSON credentials and save it to `~/google/credentials` folder.  
  d. Dont forget to Enable Compute Engine API for GCP and DataProc
2. Create a folder named `dtc_capstone_proj` and run bash shell from this folder. Clone the repo using `git clone https://github.com/AmeyKokane/DTC_Capstone_Project'
3. Go to 01_Terraform folder in your bash and run below code to provision infrastructure.

```terraform 
terraform init
terraform plan
terraform apply
```
4. We need to save Spark SQL file that is saved in 03_Apache_Spark in a folder on GCS bucket. This file will be used by DataProc task in our Airflow DAG. Use below code to move this file from local folder to GCS bucket.
```bash
cd 03_Apache_Spark \
gsutil cp spark_sql_dataproc_v2.py gs://ENTER-YOUR-GCP-BUCKEt_NAME/dataproc/spark_sql_dataproc_v3.py
```
5. Go to 02_Airflow folder, build the docker image using dockerfile. Then run the docker container. In your web browser go to `localhost:8080` to access Airflow Webserver. Run the DAG and voila~

## Future Development Roadmap:


