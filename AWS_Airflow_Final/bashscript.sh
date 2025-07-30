#!/bin/bash
sudo apt-get update
sudo apt install python3.12-venv
python3 -m venv airflow-env
source airflow-env/bin/activate
pip install apache-airflow
pip install xmlsec
sudo apt install -y pkg-config libxml2-dev libxmlsec1-dev libxmlsec1-openssl python3-dev gcc
pip install --upgrade pip setuptools wheel setuptools_scm
pip install apache-airflow-providers-amazon
pip install apache-airflow-providers-postgres
airflow standalone
sudo apt update

sudo apt install postgresql postgresql-contrib

sudo -i -u postgres

Psql

CREATE DATABASE airflow;
CREATE USER airflow WITH PASSWORD 'airflow';
GRANT ALL PRIVILEGES ON DATABASE airflow TO airflow;

\c airflow

ALTER SCHEMA public OWNER TO airflow;

GRANT USAGE, CREATE ON SCHEMA public TO airflow;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO airflow;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO airflow;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON TABLES TO airflow;

Exit

source airflow-env/bin/activate

export AIRFLOW__CORE__SQL_ALCHEMY_CONN='postgresql+psycopg2://airflow:airflow@localhost:5432/airflow'

Ls

Cd airflow

Mkdir dags

Cd dags

Nano user_processsing.py

#[copy paste the code from IDE]

Ctrl + s

Ctrl + x

Cd ..

Cd ..

source airflow-env/bin/activate

Airflow standalone
