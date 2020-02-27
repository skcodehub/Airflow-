# Airflow_Project

## Introduction:
##### This project's goal is create a data pipeline that loads data for a music app company from S3 into fact and dimension tables on Redshift using Airflow.

##### Airflow data pipeline has the following tasks:

##### 1. StagetoRedshiftOperator which loads data from source S3 bucket to Redshift stage tables
##### 2. LoadFactOperator to load data from Redshift staging tables to fact table
##### 3. LoadDimensionOperator to load data from Redshift staging tables to dimension tables
##### 4. DataQualityOperator to check for records with NULL column values

##### Required:
##### 1. Access to source S3 files
##### 2. Redshift database with tables created per DDL in the create_tables.sql
##### 3. Airflow
