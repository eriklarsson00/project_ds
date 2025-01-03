# Pipeline for data processing of twitter data

## Table of Contents
- [Overview](#overview)
- [Features](#features)
- [Getting Started](#getting-started)
- [Usage](#usage)
- [Contributing](#contributing)


## Overview
Creates a data processing pipeline with airflow to populate a database with data ready for visualization in superset. With airflow its possible to load Twitter data in json format and process data for sliding window analyis on text aswell as finding cooccurances of hashtags mentioned in the same tweets. The pipleline is set up ot handle large data sources with batch processing. To get the visual ui apache superset is the recommended visalization tool. However other tools as powerBI will also work. 


## Features
- Automates processing of twitter data to table format
- Lemmatizes text and applies a sliding window with different window sizes
- Processes table data into cooccurrences of mentioned tweets

## Getting Started

### Prerequisites
Ensure the following are installed on your system:
- **Docker**: [Install Docker](https://docs.docker.com/get-docker/)
- **Docker Compose**: [Install Docker Compose](https://docs.docker.com/compose/install/)
- **PostgreSQL**: Ensure a PostgreSQL database is available and running.

### Steps to Install and Run

1. **Clone the Repository**:
   ```bash
   git clone https://github.com/your-repo/project-name.git
   cd project-name

2. configure database.ini
host= host.docker.internal
database=xxx
user=xxx
port=5432
password=xxx

3. cd build
run build_docker.sh

4. docker compose up --build

5. access localhost:8080
activate the following dags:
start orchestration dag

