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

def CountryInfoProducer():
    api = API()

    kafka_Info = Kafka("CountryInfo")
    data = api.getAllCountrySummaries()

    for x in data:
        d = x["Premium"]["CountryStats"]
        msg = str(x["NewConfirmed"]) + "," + str(x["TotalConfirmed"]) + "," + str(x["NewDeaths"]) + "," + str(x["TotalDeaths"]) + "," + str(d["Population"]) + "," + str(d["CvdDeathRate"]) + "," + str(x["Date"][0:10])
        if d["Country"] is not "":
            kafka_Info.produce(d["Country"], msg)

def CountryCasesProducer():
    api = API()
    kafka_Cases = Kafka("CountryCases")

    countries = api.getAllCountryNames()

    i = 0
    numCountries = len(countries)

    for c in countries:
        print(str(i) + "/" + str(numCountries) + " " + c["Slug"] + "...", end=" ")
        i += 1
        if(c["Slug"] == "réunion" or c["Slug"] == "saint-barthélemy"): # These guys is problem, also empty anyways...
            continue
        data = api.getDayOneCountry(c["Slug"])
        for d in data:
            msg = str(d["Confirmed"]) + "," + str(d["Active"]) + "," + str(d["Deaths"]) + "," + str(d["Date"][0:10])
            kafka_Cases.produce(d["Country"], msg)
        print("Done")

def ProvinceCasesProducer():
    api = API()
    kafka_Cases = Kafka("ProvinceCases")

    countries = api.getAllCountryNames()

    i = 0
    numCountries = len(countries)

    for c in countries:
        print(str(i) + "/" + str(numCountries) + " " + c["Slug"])# + " provinces...", end=" ")
        i += 1
        if(c["Slug"] == "réunion" or c["Slug"] == "saint-barthélemy" or c["Slug"] == "united-states"): # These guys is problem, also empty anyways...
            continue
        data = api.getCountryProvinceCases(c["Slug"])
        for d in data:
            key = d["Country"].replace(',', '')
            province = key = d["Province"].replace(',', '')
            msg = province + "," + str(d["Lat"]) + "," + str(d["Lon"]) + "," + str(d["Confirmed"]) + "," + str(d["Active"]) + "," + str(d["Deaths"]) + "," + str(d["Date"][0:10])
            kafka_Cases.produce(key, msg)
        print("Done")

def ProvinceCasesProducerTest():
    api = API()
    kafka_Cases = Kafka("ProvinceCases")
    data = api.getCountryProvinceCases("united-states")
    print(data)
    for d in data:
        key = d["Country"].replace(',', '')
        province = key = d["Province"].replace(',', '')
        msg = province + "," + str(d["Lat"]) + "," + str(d["Lon"]) + "," + str(d["Confirmed"]) + "," + str(d["Active"]) + "," + str(d["Deaths"]) + "," + str(d["Date"][0:10])
        kafka_Cases.produce(key, msg)

if __name__ == "__main__":
    CountryInfoProducer()
    CountryCasesProducer()
    ProvinceCasesProducer()