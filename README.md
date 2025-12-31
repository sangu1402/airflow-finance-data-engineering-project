Yahoo Finance Stock Market Data Pipeline

A production-grade ELT data engineering pipeline that extracts stock market data from the Yahoo Finance API, processes it using Apache Spark, stores it in a data warehouse, and visualizes insights through interactive dashboards.

📊 Project Overview

This project implements an end-to-end automated data pipeline orchestrated using Apache Airflow on Astronomer. It fetches daily stock prices, stores raw data in object storage, transforms it using distributed Spark jobs, loads it into a data warehouse, and enables analytics via business intelligence dashboards.

The pipeline is designed to be scalable, observable, and production-ready, following modern data engineering best practices.

🚀 Key Features

Automated daily ingestion of stock market data from Yahoo Finance

Object storage using MinIO (S3-compatible)

Distributed data transformation using PySpark

Workflow orchestration with Apache Airflow & Astro

Containerized Spark jobs using DockerOperator

Data warehousing in PostgreSQL

Business intelligence dashboards with Metabase

🏗️ Architecture
Yahoo Finance API
        ↓
Apache Airflow (Astronomer)
        ↓
MinIO (Raw JSON Storage)
        ↓
Apache Spark (Dockerized Transformation)
        ↓
PostgreSQL Data Warehouse
        ↓
Metabase Dashboards

🧰 Tech Stack

Orchestration: Apache Airflow (Astronomer Runtime)

Data Processing: Apache Spark (PySpark)

Storage: MinIO (S3-compatible object storage)

Data Warehouse: PostgreSQL

Visualization: Metabase

Containerization: Docker & Docker Compose

Languages & Libraries: Python, Astro SDK

🔄 Pipeline Workflow

API Availability Sensor – Monitors Yahoo Finance API health

Extract Stock Prices – Fetches historical OHLCV data

Store Raw Data – Saves JSON files in MinIO

Transform Data – Runs Spark jobs in Docker containers

Load to Data Warehouse – Loads CSV data into PostgreSQL

Visualize – Displays insights in Metabase dashboards

📁 Data Storage Structure
stock-market/
└── NVDA/
    ├── prices.json
    └── formatted_prices/
        └── *.csv

📈 Use Cases

Track daily and historical stock price trends

Analyze trading volume and price movement

Build BI dashboards for financial insights

Learn production-grade data engineering workflows

🔐 Notes

This setup is intended for local development and learning.
For production deployments, additional security, scaling, and monitoring configurations are required.

🤝 Acknowledgments

Inspired by the Udemy course by Marc Lamberti and built using modern data engineering tools and best practices.
