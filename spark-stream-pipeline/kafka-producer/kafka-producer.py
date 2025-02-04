from kafka import KafkaProducer
import json
import uuid
import time
import random
import string

KAFKA_BROKER = "kafka:9092"  # Use service name from docker-compose
KAFKA_TOPIC = "my_topic"

time.sleep(30)
# Initialize Kafka Producer
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda x: json.dumps(x, ensure_ascii=False).encode('utf-8')
)

# Function to Generate Unique Messages
def generate_message():
    return {
        "id": str(uuid.uuid4()),  # Unique ID for each message
        "name": "sensor1",
        "value": str(round(20 + 10 * (uuid.uuid4().int % 10), 2)),  # Simulated random sensor value
        "timestamp": time.strftime("%Y-%m-%d %H:%M:%S")  # Current time
    }

def generate_large_test_message():
    """Generates a large test message matching the Debezium format"""
    current_time_ms = int(time.time() * 1000)
    micro_time = int(time.time() * 1_000_000)
    
    message = {
        "schema": {
            "type": "struct",
            "fields": [
                {
                    "type": "struct",
                    "fields": [
                        {"type": "int64", "optional": True, "name": "io.debezium.time.Timestamp", "version": 1, "field": "Datetime"},
                        {"type": "int64", "optional": True, "name": "io.debezium.time.MicroTime", "version": 1, "field": "Start_Time"},
                        {"type": "int64", "optional": True, "name": "io.debezium.time.MicroTime", "version": 1, "field": "End_Time"},
                        {"type": "float", "optional": True, "field": "Batch_No"},
                        {"type": "int32", "optional": True, "field": "Lead_Oxide"},
                        {"type": "float", "optional": True, "field": "Density"},
                        {"type": "float", "optional": True, "field": "Pentration"},
                        {"type": "string", "optional": True, "field": "Rp_Type_Mixer"}
                    ],
                    "optional": True,
                    "name": "ms-server.abd_ajna.dbo.ABD1_Mixer_3.Value",
                    "field": "after"
                }
            ],
            "optional": False,
            "name": "ms-server.abd_ajna.dbo.ABD1_Mixer_3.Envelope",
            "version": 2
        },
        "payload": {
            "before": None,
            "after": {
                "Datetime": current_time_ms,
                "Start_Time": micro_time - 1_000_000,
                "End_Time": micro_time,
                "Batch_No": round(random.uniform(150000, 160000), 2),
                "Lead_Oxide": random.randint(900, 1000),
                "Density": round(random.uniform(4.5, 5.0), 2),
                "Pentration": round(random.uniform(30.0, 40.0), 1),
                "Rp_Type_Mixer": random.choice(["Auto", "Manual", "Semi-Auto"])
            },
            "source": {
                "version": "3.0.5.Final",
                "connector": "sqlserver",
                "name": "ms-server",
                "ts_ms": current_time_ms,
                "snapshot": "false",
                "db": "abd_ajna",
                "schema": "dbo",
                "table": "ABD1_Mixer_3"
            },
            "op": "c",  # 'c' for create (insert)
            "ts_ms": current_time_ms
        }
    }
    
    #return json.dumps(message)
    return message

# Send Messages Continuously
while True:
    message = generate_large_test_message()
    producer.send(KAFKA_TOPIC, value=message)
    print(f"Sent: {message}")
    #time.sleep(10)  # Send a new message every 5 seconds

producer.close()
