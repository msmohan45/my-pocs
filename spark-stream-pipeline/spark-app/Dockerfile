FROM bitnami/spark:latest

WORKDIR /app

COPY kafka_to_postgres.py /app/
COPY requirements.txt /app/

RUN pip install -r requirements.txt

CMD ["spark-submit", "--packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0,org.postgresql:postgresql:42.5.0", "kafka_to_postgres.py"]
