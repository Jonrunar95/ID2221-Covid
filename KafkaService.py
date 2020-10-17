from kafka import KafkaProducer
from kafka import KafkaConsumer
from json import dumps, loads
from time import sleep
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
            value_deserializer = lambda x: x.decode('utf-8'),
            key_deserializer = lambda x: x.decode('utf-8')
        )
            
    def onSendSuccess(self, message):
        #print("Success:", message)
        pass

    def onSendError(self, message):
        print("Error, could not produce message:", message)

    # Produce message to topic
    def produce(self, key, value):
        self.producer.send(self.topic, key=key, value=value).add_callback(self.onSendSuccess).add_errback(self.onSendError)
        sleep(0.005)

    # Consumes earliest messages in topic, just prints it for now...
    def consume(self):
        print("Starting consumer...")
        for message in self.consumer:
            print(message.key, message.value)
        print("Timeout: Stopping consumer")

if __name__ == "__main__":
    api = API()
    kafka_Cases = Kafka("CountryCases")
    kafka_Info = Kafka("CountryInfo")

    # Get information on each country
    data = api.getAllCountrySummaries()
    for x in data:
        # Just get relevant info from json object
        d = x["Premium"]["CountryStats"]
        #'Population', 'PopulationDensity', 'MedianAge', 'Aged65Older', 'Aged70Older', 'ExtremePoverty', 'GdpPerCapita', 'CvdDeathRate', 'DiabetesPrevalence', 
        #'HandwashingFacilities', 'HospitalBedsPerThousand', 'LifeExpectancy', 'FemaleSmokers', 'MaleSmokers'}
        msg = str(d["Population"]) + "," + str(d["PopulationDensity"]) + "," + str(d["MedianAge"]) + "," + str(d["LifeExpectancy"])
        kafka_Info.produce(d["Country"], msg)


    # Produce all daily cases for iceland
    data = api.getDayOneCountry("iceland")
    for d in data:
        # Just get relevant info from json object
        msg = str(d["Confirmed"]) + "," + str(d["Deaths"]) + "," + str(d["Active"]) + "," + str(d["Date"][0:10])
        kafka_Cases.produce(d["Country"], msg)

    kafka_Info.consume()
    kafka_Cases.consume()
    
