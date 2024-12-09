# Use the pulled Airflow image as the base
FROM apache/airflow:2.10.3

# Install additional Python dependencies
COPY Requirements.txt /tmp/Requirements.txt
RUN pip install -r /tmp/Requirements.txt


