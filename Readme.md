# airflow_etl_stock_pipeline

An airflow pipeline for daily updates on selected stock market companies news and prices.

## Table of contents

* [Introduction](#Introduction)
* [Technologies](#Technologies)
* [Setup and usage](#setup-and-usage)
* [Features](#features)
* [Author](#Author)

### Introduction

This project leverages Apache Airflow to extract, transform, and load prominent data from two stock market APIs. The pipeline is designed to pull historical price data and news articles related to any specific stock or stocks of interest.
This pipeline uses Postgres for the staging, and IBM Cloudant as final NOSQL storage database. The structure of this project makes it easy to implement additional APIs if necessary.

### Technologies and libraries

* Python 3.9
* Airflow
* Postgres
* Docker
* Python-dotenv
* IbmCloudant
* Requests
* Jinja2
* Newsapi-Python

### Setup and usage

***Setup***

The best way to use this project is to build an image "myairflow" using the provided dockerfile:
`docker build -t myairflow .`
`docker-compose up` to start the containers. The rest is set up, except for the connection's apikey you will need to provide in the .env file.

Alternatively, if you have an existing instance of airflow, you can clone the Include folder into your airflow. Make sure to define
pythonpath variable leading to the folder:

`ENV PYTHONPATH /path/to/airflow/include`

Add additional dag folder to your airflow by modifying .env:
`AIRFLOW__CORE__DAGS_FOLDER=/path/to/airflow/include/custom_dags`

This project requires a postgres database to work. You can specify your postgres connection settings in .env file, described in the next section.

***Usage***

This pipeline features 3 dags to extract, load and transform stock-related data. 

`postgres_init_dbs` creates a postgres
database for staging, and required tables. This dag is run once, and is included for convenience. 

`populate` provides initial data population, based on .env settings. This dag is run once.

`update` performs data update daily. It also checks for the latest update date of information in Cloudant database, and will fill any gaps that might exist.

***.env settings***
```
symbol: 
List of tickers to acquire the data for. All upper case. Ex: ["GOOG","AAPL"]

timezone: 
Timezone to use when quering APIs. Default is utc.

start_date:
The date from which "populate" dag will collect information from. "YYYY-MM-DD" format.

end_date: 
The date to which "populate" dag will collect information to. "YYYY-MM-DD" format.
Use only if you are not planning on using "Update" dag, as it will attempt to get the most recent data.

interval:
The data frequency interval. Default is 1day. Supports: 1min, 5min, 15min, 30min, 45min, 1h, 2h, 4h, 1day, 1week, 1month.
Used only for stock prices. If changed, make sure to change the schedule of "update" dag accordingly. Other intervals haven't been tested.

AIRFLOW_CONN_CLOUDANT:
Your IBM Cloudant IAM authorization settings. You can find more information here: https://cloud.ibm.com/docs/Cloudant?topic=Cloudant-faq-authenticating-cloudant
Ex:
'{
    "conn_type": "Cloudant",
    "login": "Cloudant database url",
    "password": "Your cloudant API key"
}'

AIRFLOW_CONN_TWELVEDATA:
Airflow's Twelvedata API connection information. You only need to input your apikey.
Ex:
'{
    "conn_type": "TwelveData",
    "host": "https://api.twelvedata.com/",
    "login": "True",
    "password": "Your API key"
}'

AIRFLOW_CONN_NEWSAPI: 
Airflow's Twelvedata API connection information. You only need to input your apikey.
'{
    "conn_type": "TwelveData",
    "host": "https://newsapi.org/v2/",
    "login": "True",
    "password": "Your API key"
}'

AIRFLOW_CONN_POSTGRES: 
Airflow's postgres connection information. Leave default is using provided docker image, or input your own.
'{
    "conn_type": "postgres",
    "port": "5432",
    "host": "ipop_airflow_stock_pipeline-postgres-1",
    "login": "airflow",
    "password": "airflow"
}'

```

After the setup, simply unpause the required dags.


### Features

* Full Airflow ETL pipeline
* Acquisition of stock news and price data through Rest-API
* Utilizes NOSQL databases for staging and warehousing
* Custom Airflow hooks and operators for clean and expandable project and code
* Data transformation into a common JSON structure for ease of access and manipulation

### Project Status
This project is operational, but incomplete.

Future steps to complete the project:

* Testing additional parameters
* Catching any un-foreseen errors
* Creating more fail-safes in case DAG errors-out.

### Author

Created by Ivan Popov

