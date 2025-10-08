# WorkShoop2-ETL
This is an academic project that uses Docker and Apache Airflow to perform the integration of two datasets extracted from the Kaggle platform. The project implements a complete ETL (Extract, Transform, Load) process, demonstrating the orchestration of data pipelines using containerized environments.
## 🎵 Data Engineering & Analytics Pipeline with Airflow, Docker, MySQL, and Metabase

This project demonstrates the creation of a data pipeline for processing, cleaning, and visualizing datasets using Apache Airflow, Docker, MySQL, and Metabase.
It integrates two datasets from Kaggle, performs ETL (Extract, Transform, Load) processes, builds a data warehouse, and finally creates visual dashboards with key KPIs.
<img width="1011" height="476" alt="image" src="https://github.com/user-attachments/assets/f5cd3973-8cf5-47c8-ad0f-7d5e69203ca8" />


### 🚀 Project Overview

The objective of this project is to automate a complete data workflow — from raw data ingestion to business intelligence dashboards — using containerized environments and modern data tools.

### Main components:

Docker & Docker Compose – container orchestration

Apache Airflow – ETL automation and scheduling

MySQL Workbench – local data warehouse

Metabase – KPI visualization and analytics dashboard

### 📦 Step 1: Install Requirements

Before running the project, make sure you have the following installed:

    Docker Desktop
    
    WSL 2
     (if using Windows)
    
    Python libraries (used in notebooks and DAGs):
    
    pip install pandas sqlalchemy apache-airflow matplotlib seaborn

## ⚙️ Step 2: Create the Docker Compose File

Create a file named docker-compose.yml in your project root.
This file will define the services (containers) for Airflow, MySQL, and Metabase.

###Example structure:

    version: '3'
    services:
      mysql:
        image: mysql:latest
        environment:
          MYSQL_ROOT_PASSWORD: root
          MYSQL_DATABASE: musicdb
        ports:
          - "3306:3306"
        volumes:
          - ./data/mysql:/var/lib/mysql
    
      airflow:
        image: apache/airflow:2.6.0
        ports:
          - "8080:8080"
        volumes:
          - ./dags:/opt/airflow/dags
        environment:
          - AIRFLOW__CORE__EXECUTOR=LocalExecutor
          - AIRFLOW__CORE__SQL_ALCHEMY_CONN=mysql+mysqlconnector://root:root@mysql:3306/musicdb
        depends_on:
          - mysql
    
      metabase:
        image: metabase/metabase
        ports:
          - "3000:3000"
        depends_on:
          - mysql
    

**Run the containers:**

    docker-compose up -d


**Now you can access:**

Airflow UI: http://localhost:8080

Metabase UI: http://localhost:3000

### 🎧 Step 3: Datasets Used

**Two datasets were extracted from Kaggle:**

**Spotify Dataset** – contains song-level information:

artist, track_name, popularity, duration_ms, explicit,
danceability, energy, key, loudness, mode, speechiness,
acousticness, instrumentalness, liveness, valence,
tempo, time_signature, track_genre


**Awards Dataset** – contains information about music awards:

year, title, published_at, updated_at, category,
nominee, artist, workers, img, winner

## 🧠 Step 4: Data Exploration

In the notebooks/ folder, a preliminary Exploratory Data Analysis (EDA) was performed using pandas, matplotlib, and seaborn to understand:

Data distribution and missing values

Outliers and variable correlations

Basic descriptive statistics

## 🧹 Step 5: Data Cleaning with Airflow

In the dags/ folder, an Airflow DAG automates the cleaning process for one of the datasets.
This DAG:

Reads raw data

Applies transformations and cleaning

Merges both datasets

Loads the cleaned data into MySQL and exports it as a .csv file to a local or cloud storage drive

## 🗄️ Step 6: Building the Data Warehouse

The cleaned and merged data is stored in MySQL Workbench as a data warehouse.
This allows structured queries and supports Metabase connections for visualization.

## 📊 Step 7: Visualization with Metabase

A dashboard was created in Metabase (running on port 3000) to visualize KPIs and correlations between the datasets.
It includes metrics such as:

Most popular artists and songs

Trends in award winners by year

Relationship between song popularity and danceability, energy, and valence

## 🧩 Technologies Used

Docker / Docker Compose	Environment: orchestration
Apache Airflow: Workflow automation
MySQL: Data storage and warehouse
Metabase: Data visualization
Pandas, SQLAlchemy, Matplotlib, Seaborn	Data: analysis and visualization
## 🏁 Final Notes

This project demonstrates the full lifecycle of a data engineering and analytics pipeline, from raw data ingestion to insightful dashboards.
It can be extended by integrating APIs, cloud storage, or automated updates via Airflow scheduling.
