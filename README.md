# German Elections Big Data Pipeline

End-to-end data engineering project for German election and political activity data.

## Goal

This is a non-commercial educational portfolio project.

The goal is to build a complete data engineering pipeline that combines official German election data with public political discussion data and prepares it for analytical reporting.

## Technologies

- Azure Data Factory
- Azure Data Lake Storage
- Apache Spark / PySpark
- Apache Airflow
- SQL / MySQL
- Power BI / Tableau

## Reddit API Usage

The project intends to use the Reddit API in read-only mode to collect a limited amount of publicly available posts and comments from selected German political and election-related subreddits.

The data will be used for aggregate analysis such as:

- discussion volume over time
- engagement metrics
- election-related topic trends
- comparison of discussion activity around political parties and elections

The project will not:

- post or interact with Reddit users
- vote or moderate content
- profile individual users
- infer political affiliation or other sensitive attributes of individual users
- train AI models using Reddit data
- resell or redistribute Reddit data

The collected data will be processed through an external data engineering pipeline using Azure Data Factory, Apache Spark, SQL, and BI tools.
