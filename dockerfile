FROM apache/airflow:2.9.3
COPY requirements.txt /requirements.txt
RUN pip install --upgrade pip
RUN pip install --no-cache-dir  -r /requirements.txt