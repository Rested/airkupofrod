FROM puckel/docker-airflow
COPY ./dags /usr/local/airflow/dags
USER root
RUN apt-get update && \
    apt-get upgrade -y && \
    apt-get install -yqq git && \
    pip install git+https://github.com/rekon-oss/airkupofrod.git
USER airflow