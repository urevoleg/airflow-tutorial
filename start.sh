#!/bin/bash
cd /home/userad/projects/ya-tracker-dash && source venv/bin/activate
airflow webserver --port 8080 && airflow scheduler