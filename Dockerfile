FROM puckel/docker-airflow:1.10.6-1
ENV AIRFLOW_HOME=/usr/local/airflow

COPY requirements.txt ./requirements.txt
COPY entrypoint.sh ./entrypoint.sh

RUN pip install -r requirements.txt --user
RUN touch __init__.py

# ENTRYPOINT ["/entrypoint.sh"]

# uncomment next 2 lines if you want to use 'docker-compose-volume-packages.yml'
# RUN mkdir /usr/local/airflow/packages
# COPY ./packages.pth /usr/local/lib/python3.7/site-packages

# COPY ./airflow.docker.cfg /usr/local/airflow/airflow.cfg
# or change to ./airflow_celeryexecutor.cfg /usr/local/airflow/airflow.cfg
# if you need .cfg with CeleryExecutor
