FROM apache/airflow:latest-python3.9
COPY requirements.txt /
RUN pip install --no-cache-dir -r /requirements.txt

# Copy the wheel file to the Docker image
COPY ipop_airflow_stock_pipeline /opt/airflow/ipop_airflow_stock_pipeline

ENV PYTHONPATH /opt/airflow/ipop_airflow_stock_pipeline

