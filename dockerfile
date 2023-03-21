FROM apache/airflow:latest-python3.9
COPY requirements.txt /
RUN pip install --no-cache-dir -r /requirements.txt


COPY include /opt/airflow/include
ENV PYTHONPATH /opt/airflow/include

