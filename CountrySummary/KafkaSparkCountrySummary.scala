package sparkstreaming

import java.util.HashMap
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka._
import kafka.serializer.{DefaultDecoder, StringDecoder}
import org.apache.spark.SparkConf
import org.apache.spark
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka._
import org.apache.spark.storage.StorageLevel
import java.util.{Date, Properties, Calendar, GregorianCalendar}
import java.text.SimpleDateFormat; 
import java.lang.Math;
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord, ProducerConfig}
import scala.util.Random
import org.joda.time.DateTime
import org.apache.spark.sql.cassandra._
import com.datastax.spark.connector._
import com.datastax.driver.core.{Session, Cluster, Host, Metadata}
import com.datastax.spark.connector.streaming._

import org.apache.spark.streaming.{Seconds, StreamingContext}



object KafkaSparkAllCountries {
  def main(args: Array[String]) {

    case class CountryData(newconfirmed: Int, totalconfirmed: Int, newdeaths: Int, totaldeaths: Int, population: Int, date: Date)
    
    val cluster = Cluster.builder().addContactPoint("127.0.0.1").build()
    val session = cluster.connect()

    // connect to Cassandra and make a keyspace and table
    session.execute("CREATE KEYSPACE IF NOT EXISTS covid WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };")
    session.execute("CREATE TABLE IF NOT EXISTS covid.countrydata (country text PRIMARY KEY, newconfirmed float, totalconfirmed float, newdeaths float, totaldeaths float, population float, date float);")
    // make a connection to Kafka and read (key, value) pairs from it
    val kafkaConf = Map(
      "metadata.broker.list" -> "localhost:9092",
      "zookeeper.connect" -> "localhost:2181",
      "group.id" -> "kafka-spark-streaming",
      "zookeeper.connection.timeout.ms" -> "1000"
    )

    val conf =  new SparkConf().setAppName("CountryInfo").setMaster("local");
    val ssc = new StreamingContext(conf, Seconds(1))
    val sc = ssc.sparkContext
    sc.setLogLevel("ERROR")
    ssc.checkpoint("checkpointDirectory")

    val topicsSet = Set("CountryInfo")
    val kafkaStream  = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder]( ssc, kafkaConf, topicsSet)

    val messages = kafkaStream.map { x =>
      val data = x._2.split(",")
      val dateString = data(5).split("-")
      //val date = new GregorianCalendar(dateString(0).toInt, dateString(1).toInt -1, dateString(2).toInt).getTime()
      //val covidData = CovidData(data(0).toInt, data(1).toInt, data(2).toInt, data(3).toInt, data(4).toInt, date)
      (x._1, data(0).toInt, data(1).toInt, data(2).toInt, data(3).toInt, data(4).toInt)
    }


    // store the result in Cassandra
    messages.foreachRDD { rdd =>
      rdd.saveToCassandra("covid", "countrydata", SomeColumns("country", "newconfirmed", "totalconfirmed", "newdeaths", "totaldeaths", "population"))
    }
    
    ssc.start()
    ssc.awaitTermination()
  }
}
