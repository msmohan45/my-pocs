FROM python:3.9

WORKDIR /app

COPY kafka-producer.py /app/
COPY wait-for-kafka.sh /app/
COPY requirements.txt /app/

RUN chmod +x /app/wait-for-kafka.sh

RUN pip install -r requirements.txt

CMD ["python", "/app/kafka-producer.py"]
