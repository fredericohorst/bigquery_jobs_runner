FROM edusouza/pandas_bigquery:latest

MAINTAINER Frederico Horst

RUN mkdir /bigquery_jobs_runner

WORKDIR /bigquery_jobs_runner

COPY . .

CMD ["python3", "src/main.py"]
