FROM apache/airflow:2.8.1-python3.9
USER root
RUN pip install snowflake-connector-python
USER airflow