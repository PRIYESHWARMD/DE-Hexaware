
from azure.eventhub import EventHubProducerClient, EventData
import json, datetime, random, time

producer = EventHubProducerClient.from_connection_string(
    conn_str="EVENT_KEY",
    eventhub_name="traffic-iot"
)

while True:
    data = {
        "vehicle_id": random.randint(1000, 9999),
        "speed": random.randint(20, 120),
        "timestamp": str(datetime.datetime.now())
    }
    event = EventData(json.dumps(data))
    with producer:
        producer.send_batch([event])
    print("Sent:", data)
    time.sleep(2)
