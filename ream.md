# Project: Data Pipelines using Apache Airflow 

<p> A music streaming company, Sparkify, has decided that it is time to introduce more automation
and monitoring to their data warehouse ETL pipelines and come to the conclusion that the best 
tool to achieve this is Apache Airflow. </p>

I am tasked with the creation of high grade data pipelines that are dynamic and built from reusable 
tasks that can be monitored and allow for easy backfills.The pipeline should include data quality 
checks against the datasets after initiating the ETL process to monitor any discrepancies in the 
datasets.

The source data resides in S3 and needs to be processed in Sparkify's data warehouse in Amazon 
Redshift. The source datasets consist of JSON logs that tell about user activity in the application 
and JSON metadata about the songs the users listen to.

## Data Model using Star Schema

    
    ![ Alt song_play_analysis_with_star_schema.png](https://github.com/nogun096/DataPipeline/blob/9b5608af7c27ec7c527d91a012937eb35125c79d/image/song_play_analysis_with_star_schema.png)


AIRFLOW SETUP 

The requirements for the airflow setup includes Anaconda with python3.7, docker, docker compose 
AWS account and redshift cluster. 
A setup of the airflow locally was done by installing both docker and apache-airflow on my 
local system after which the airflow was initialized using "airfow db init" via the terminal. 
Running both the scheduler and webserver to be able to view the airflow ui via the web page using 
localhost:8080

AWS IaM credentials to Airflow

In order to run AWS commands in Airflow tasks you will have to add your AWS credentials to Airflow.
This can be done from the Airflow web GUI:

Select Admin->Connection and click on the Create tab
aws_credentials in the Conn_Id field
Amazon Web Services in the Conn Type field
AWS_ACCESS_KEY_ID in the Login field
AWS_SECRET_ACCESS_KEY in the Password

Set redshift connection settings in Airflow

Similar process is carried out for the redshift connnection. 

Setup Database
In order to create the tables in Redshift, I have included a DAG setup_dag.py 
that will create the necessary tables.

You may wish to edit the etl_dag.py DAG to alter the parameters like start_date 
and scheduled_interval.

Execution

Enable the DAG in Airflow and it should begin processing.
