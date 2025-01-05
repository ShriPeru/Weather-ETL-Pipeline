# Weather ETL Pipeline

## Overview
This project demonstrates a comprehensive Weather ETL (Extract, Transform, Load) pipeline leveraging modern data engineering tools and technologies. The pipeline integrates data ingestion, transformation, storage, and analysis workflows to process weather data efficiently.

## Technologies Used
- **Languages and Tools:** Python, SQL, Jupyter Notebook, Unix Shell
- **Frameworks and Services:** PySpark, AWS (S3, EC2, RDS), PostgreSQL

## Features
- **End-to-End ETL Workflow:** 
  - Designed and implemented an ETL workflow from scratch using AWS and PySpark to ingest, transform, and store weather data in a PostgreSQL relational database.
  
- **Automation with Crontab:** 
  - Automated data ingestion workflows using `crontab` on an AWS EC2 instance. Scheduled batch scripts to ingest data into an S3 bucket, followed by transformation and loading processes. The automation included detailed logging for tracking operations.

- **Data Processing and Transformation:** 
  - Processed JSON files in PySpark DataFrames, performing data transformations, aggregations, and loading results into an AWS-hosted PostgreSQL database with interrelated tables.

- **SQL-Driven Analytics:** 
  - Queried and visualized relational database data to generate insights using SQL.

## Key Achievements
- Successfully processed and managed weather data with efficient data engineering practices.
- Built robust automation for seamless daily ingestion and processing of data.
- Enabled actionable insights through SQL-based analytics and visualizations.

## Usage
1. Configure AWS credentials for S3, EC2, and RDS instances.
2. Use crontab to schedule batch scripts for automated data ingestion and transformation.
3. Query and visualize the data using SQL for insights and analysis.

---

This project highlights skills in data engineering, automation, and data analysis using cloud services and big data frameworks.
