# Crypto Data Pipeline with Apache Airflow

## Overview

Welcome to the **Crypto Data Pipeline**! This project uses **Apache Airflow** to schedule and automate the extraction of live cryptocurrency data from a public API every minute. The data is processed and stored in a database, ready for analysis or real-time usage.

### Key Features
- **Live Data Collection**: Pulls live data from the Crypto API every minute.
- **Data Storage**: Saves the data directly into a database for easy access and analysis.
- **Airflow Scheduling**: Uses Apache Airflow to schedule and manage tasks with ease.
- **Dockerized Deployment**: Fully containerized with Docker for easy setup and scalability.
- **Easy Configuration**: Configure and customize your setup with minimal effort using the `docker-compose.yml` file.

## How It Works
1. **API Integration**: Fetches live cryptocurrency data from a public crypto API.
2. **Data Processing**: Processes and transforms the data.
3. **Database Storage**: Stores the data into a designated database (e.g., PostgreSQL, MySQL).
4. **Scheduled Runs**: Airflow schedules the task to run every minute for continuous data collection.

## Project Setup

### Prerequisites
Before getting started, ensure you have:
- Docker
- Docker Compose
- Python 3.11
