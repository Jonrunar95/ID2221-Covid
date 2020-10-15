from kafka import KafkaProducer
from kafka import KafkaConsumer
from json import dumps, loads
from CovidAPI import *

# Kafka instance for specific topic
# topic: String
class Kafka():
    def __init__(self, topic):
        self.topic = topic
        self.producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
                         value_serializer=lambda x: 
                         dumps(x).encode('utf-8'))
        self.consumer = KafkaConsumer(
            topic,
            bootstrap_servers=['localhost:9092'],
            auto_offset_reset='earliest',
            enable_auto_commit=True,
            group_id='my-group',
            request_timeout_ms = 15000, # Not sure about this
            consumer_timeout_ms = 20000, # How long until we stop consumer
            value_deserializer=lambda x: loads(x.decode('utf-8')))
            

    # Produce message to topic
    def produce(self, message):
        self.producer.send(self.topic, value=message)

    # Consumes earliest messages in topic, just prints it for now...
    def consume(self):
        print("Starting consumer...")
        for message in self.consumer:
            print(message.value)
        print("Timeout: Stopping consumer")

if __name__ == "__main__":
    api = API()
    kafka = Kafka("test")

    data = api.getDayOneCountry("iceland")

    # Produce all daily cases for iceland
    for d in data:
        # Just get relevant info from json object
        msg = {"Country": d["Country"], "Confirmed": d["Confirmed"], "Deaths": d["Deaths"], "Active": d["Active"], "Date": d["Date"]}
        kafka.produce(msg)

    kafka.consume()





