FROM puckel/docker-airflow:1.10.6-1
ENV AIRFLOW_HOME=/usr/local/airflow

USER root
RUN apt-get update && apt-get install -y python-dev python3-dev \
    build-essential libssl-dev libffi-dev \
    libxml2-dev libxslt1-dev zlib1g-dev wget
USER airflow

RUN pip install --user lxml Cython psycopg2-binary
COPY requirements.txt ./requirements.txt

RUN pip install -r requirements.txt --user
RUN touch __init__.py
# RUN python -m spacy download en_core_web_sm
# RUN pip install --upgrade pip
RUN wget https://github.com/explosion/spacy-models/releases/download/en_core_web_sm-2.2.0/en_core_web_sm-2.2.0.tar.gz
RUN pip install --user en_core_web_sm-2.2.0.tar.gz
RUN rm en_core_web_sm-2.2.0.tar.gz
