# SPARKIFY AIRFLOW

## Airflow Pipeline To Fetch JSON Files from s3 and transform them into a series of queriable tables in AWS Redshift

### Project Overview

Music company sparkify generate JSON logs that cover how songs are played in their app. This dataset is joined with an open source songs and artist JSON collection so data analysts can identify trends in song plays. Apache airflow is used as the orchestration tool for the pipeline.


### App Architecture

![App Architecture Diagram](diagrams/airflow_uml.jpg)