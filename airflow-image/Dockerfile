FROM puckel/docker-airflow
USER root
COPY airkupofrod/ ./airkupofrod
COPY setup.py LICENSE README.md ./
RUN pip install ./
USER airflow
COPY airflow-image/dags /usr/local/airflow/dags
