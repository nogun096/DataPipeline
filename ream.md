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

![song_play_analysis_with_star_schema!](image/song_play_analysis_with_star_schema.png "song_play_analysis_with_star_schema")

## Configuring the DAG

In the DAG, add default parameters according to these guidelines

1. The DAG does not have dependencies on past runs
2. On failure, the task are retried 3 times
3. Retries happen every 5 minutes
4. Catchup is turned off
5. Do not email on retry

In addition, configure the task dependencies so that after the dependencies are set, the graph view follows the flow shown in the image below.

**Configure the task dependencies**
![DAG!](image/sparkify-dag.png "sparkify-dag")

## Opearational Concept 

### Stage Operator
<p>The stage operator is expected to be able to load any JSON formatted files from S3 to Amazon Redshift. The operator creates and runs a SQL COPY statement based on the parameters provided. The operator's parameters should specify where in S3 the file is loaded and what is the target table.</p>

<p>The parameters should be used to distinguish between JSON file. Another important requirement of the stage operator is containing a templated field that allows it to load timestamped files from S3 based on the execution time and run backfills.</p>

### Fact and Dimension Operators
<p>With dimension and fact operators, you can utilize the provided SQL helper class to run data transformations. Most of the logic is within the SQL transformations and the operator is expected to take as input a SQL statement and target database on which to run the query against. You can also define a target table that will contain the results of the transformation.</p>

<p>Dimension loads are often done with the truncate-insert pattern where the target table is emptied before the load. Thus, you could also have a parameter that allows switching between insert modes when loading dimensions. Fact tables are usually so massive that they should only allow append type functionality.</p>

### Data Quality Operator
<p>The final operator to create is the data quality operator, which is used to run checks on the data itself. The operator's main functionality is to receive one or more SQL based test cases along with the expected results and execute the tests. For each the test, the test result and expected result needs to be checked and if there is no match, the operator should raise an exception and the task should retry and fail eventually.</p>

## Airflow setup

The requirements for the airflow setup includes Anaconda with python3.7, docker, docker compose 
AWS account and redshift cluster. 
A setup of the airflow locally was done by installing both docker and apache-airflow on my 
local system after which the airflow was initialized using "airfow db init" via the terminal. 
Running both the scheduler and webserver to be able to view the airflow ui via the web page using 
localhost:8080

### AWS IaM credentials to Airflow

In order to run AWS commands in Airflow tasks you will have to add your AWS credentials to Airflow.
This can be done from the Airflow web GUI:

Select Admin->Connection and click on the Create tab
![admin connections!](./image/admin-connections.png "admin connections")

aws_credentials in the Conn_Id field
Amazon Web Services in the Conn Type field
AWS_ACCESS_KEY_ID in the Login field
AWS_SECRET_ACCESS_KEY in the Password

![create connections!](./image/create-connection.png "create connections")

Set redshift connection settings in Airflow

Similar process is carried out for the redshift connnection. 

Setup Database
In order to create the tables in Redshift, I have included a DAG setup_dag.py 
that will create the necessary tables.


You may wish to edit the etl_dag.py DAG to alter the parameters like start_date 
and scheduled_interval.

Execution

Enable the DAG in Airflow and it should begin processing.
