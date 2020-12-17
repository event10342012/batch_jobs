FROM python:3.7

MAINTAINER Leo Chen

WORKDIR /opt/airflow

ENV PYTHONPATH /opt/airflow
ENV AIRFLOW_HOME /opt/airflow

COPY src/batch_job/ .

RUN useradd airflow \
    && mkdir logs \
    && chown -R airflow:airflow . \
    && chmod -R 744 .

RUN pip install --upgrade pip \
    && pip install -r requirements.txt \
    && pip cache purge

USER airflow

EXPOSE 8080 5555 8793

ENTRYPOINT ["./scripts/init.sh"]
