#!/bin/bash

docker build -t apache-airflow-spark:3.1.3 .
docker compose up -d