# Pipeline for data processing of twitter data

## Table of Contents
- [Pipeline for data processing of twitter data](#pipeline-for-data-processing-of-twitter-data)
  - [Table of Contents](#table-of-contents)
  - [Getting Started](#getting-started)
    - [Prerequisites](#prerequisites)
    - [Steps to Install and Run](#steps-to-install-and-run)
  - [File structure](#file-structure)
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

2. **Configure database.ini**:
   Create a `database.ini` file in the `config/` directory with the following content:
   ```ini
   [postgresql]
   host= host.docker.internal
   database=xxx
   user=xxx
   port=5432
   password=xxx
   ```
   Replace `host`, `database`, `user`, `port`, and `password` with your PostgreSQL database details.

3. **Build and Run the Docker Containers**:
   ```bash
   docker-compose up --build
   ```
   This command will build the Docker containers and start the services.



4. **Access the Superset UI**:
   Open your browser and go to `http://localhost:8088/` to access the Superset UI.
   Use the following credentials to log in:
   - **Username**: `admin`
   - **Password**: `admin`

5. **Copy tweets to the folder labelled 'tweets'**:
   The data will be processed from this folder so you can copy all the json to a subfolder under tweets. Make sure that the json is not directly stored in the tweets folder and has a parent folder sitting inside tweets.
   The process will break down all json into batches, but it does not keep track of whether the data is already uploaded to the DB. You can delete the files manually after completion or create a task in airflow to do the same.
6. **Start DAGs in Airflow**:
   Open your browser and go to `http://localhost:8080/` to access the Airflow UI.
   Use the following credentials to log in:
   - **Username**: `Airflow`
   - **Password**: `Airflow`

## File structure
```
project-name/
│
├── build/                 # Scripts and Dockerfiles for setting up the environment
├── code/                  # Core Python code for data processing
├── config/                # Configuration files (e.g., database.ini)
├── .env                   # Environment variables
└── docker-compose.yaml    # Configuration file for Docker Compose setup
```

## Code Contribution

### Elis Indebetou
Made slidingWindowWithOverlap function in Lemmatization.py for the data processing pipeline. Worked with Erik with hashtagNetwork.py, Erik was main contributor.

### Erik Larsson
created db connection, insertion logic for sliding window connection. worked on hashtag network with Elis. created Dag process for hashtag network. Also helped with the main processing pipeline. however the earlier iterations and not the final version. Helped lemmatization and cleanup logic and with debugging the main processing pipeline. Worked with the sql in Superset and and created plots for the processed data. 


### Balasubramani V S
Created the airflow batch pipeline for the twitter data in airflow, restructured all code to be compatible with airflow. Helped with debugging the insertion logic for sliding window connections and the sliding window logic. Also made the lemmatization preprocessing and cleanup