# Use an official Airflow image with Python 3.12.
FROM apache/airflow:2.11.2-python3.12

USER root

# Create directories used by the project.
RUN mkdir -p /opt/airflow/src \
    /opt/airflow/sql \
    /opt/airflow/data

# Give the Airflow user ownership of project directories.
RUN chown -R airflow:root \
    /opt/airflow/src \
    /opt/airflow/sql \
    /opt/airflow/data

USER airflow

# Install Python dependencies separately from project source.
COPY requirements.txt /tmp/requirements.txt

RUN pip install --no-cache-dir \
    -r /tmp/requirements.txt

# Copy application code and SQL scripts.
COPY --chown=airflow:root src/ /opt/airflow/src/
COPY --chown=airflow:root sql/ /opt/airflow/sql/
COPY --chown=airflow:root dags/ /opt/airflow/dags/

# Make the application package importable by Airflow.
ENV PYTHONPATH="/opt/airflow/src"