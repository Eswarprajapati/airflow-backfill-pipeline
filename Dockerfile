FROM apache/airflow:3.3.2
USER root
RUN mkdir -p /opt/airflow/portfolio/results && chown -R airflow:root /opt/airflow/portfolio
USER airflow
COPY --chown=airflow:root pipeline.py /opt/airflow/portfolio/pipeline.py
COPY --chown=airflow:root data /opt/airflow/portfolio/data
COPY --chown=airflow:root dags /opt/airflow/dags
ENV PYTHONPATH=/opt/airflow/portfolio
