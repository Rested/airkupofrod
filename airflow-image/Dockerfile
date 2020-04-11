FROM puckel/docker-airflow
USER root
RUN apt-get update && \
    apt-get upgrade -y && \
    apt-get install -yqq git && \
    pip install git+https://github.com/rekon-oss/airkupofrod.git@4a473af4c51319de4c0aa2d71d1e68ba8fed5dad
USER airflow
COPY ./dags /usr/local/airflow/dags
