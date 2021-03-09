FROM apache/airflow:2.0.1

ARG AIRFLOW_HOME=/opt/airflow

USER root

# install odbc and gcc
RUN curl https://packages.microsoft.com/config/debian/10/prod.list > /etc/apt/sources.list.d/mssql-release.list \
    && curl -sSL https://packages.microsoft.com/keys/microsoft.asc | sudo apt-key add - \
    && apt-get update \
    && ACCEPT_EULA=Y apt-get install msodbcsql17 \
    && ACCEPT_EULA=Y apt-get install mssql-tools \
    && echo 'export PATH="$PATH:/opt/mssql-tools/bin"' >> ~/.bash_profile \
    && echo 'export PATH="$PATH:/opt/mssql-tools/bin"' >> ~/.bashrc \
    && source ~/.bashrc \
    && apt-get install -y unixodbc-dev libgssapi-krb5-2 gcc g++ \
    && pip install --upgrade pip \
    && pip install apache-airflow[postgres,microsoft.mssql,celery,redis,rabbitmq,ftp,odbc] \
    && pip cache purge \
    && apt-get clean


USER airflow

WORKDIR ${AIRFLOW_HOME}
