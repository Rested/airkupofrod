FROM puckel/docker-airflow
USER root
RUN apt-get update && \
    apt-get upgrade -y && \
    apt-get install -yqq git && \
    pip install git+https://github.com/rekon-oss/airkupofrod.git@dad2e34889ea578fc8d908a4d5224c245d37bc99
USER airflow
COPY ./dags /usr/local/airflow/dags
