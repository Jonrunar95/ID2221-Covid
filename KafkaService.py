from kafka import KafkaProducer
from kafka import KafkaConsumer
from json import dumps, loads
from CovidAPI import *

# Kafka instance for specific topic
# topic: String
class Kafka():
    def __init__(self, topic):
        self.topic = topic
        self.producer = KafkaProducer(
            bootstrap_servers=['localhost:9092'],
            key_serializer = str.encode,#lambda x: dumps(x).encode('utf-8'),
            value_serializer = str.encode #lambda x: dumps(x).encode('utf-8')
            
        )
        self.consumer = KafkaConsumer(
            topic,
            bootstrap_servers=['localhost:9092'],
            auto_offset_reset='earliest',
            enable_auto_commit=True,
            group_id='my-group',
            request_timeout_ms = 15000, # Not sure about this
            consumer_timeout_ms = 20000, # How long until we stop consumer
            value_deserializer= lambda x: loads(x.decode('utf-8'))
        )
            

    # Produce message to topic
    def produce(self, key, value):
        print(key, ",", value)
        self.producer.send(self.topic, key=key, value=value)

    # Consumes earliest messages in topic, just prints it for now...
    def consume(self):
        print("Starting consumer...")
        for message in self.consumer:
            print(message.value)
        print("Timeout: Stopping consumer")

if __name__ == "__main__":
    api = API()
    kafka = Kafka("AllCountries")

    data = api.getDayOneCountry("iceland")

    # Produce all daily cases for iceland
    for d in data:
        # Just get relevant info from json object
        msg = str(d["Confirmed"]) + "," + str(d["Deaths"]) + "," + str(d["Active"]) + "," + str(d["Date"][0:10])
        kafka.produce(d["Country"], msg)