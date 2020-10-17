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



object KafkaSpark {
  def main(args: Array[String]) {

    case class CovidData(confirmed: Int, deaths: Int, active: Int, date: Date, last14: Int)
    
    val cluster = Cluster.builder().addContactPoint("127.0.0.1").build()
    val session = cluster.connect()


    // connect to Cassandra and make a keyspace and table
    session.execute("CREATE KEYSPACE IF NOT EXISTS covid WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };")
    session.execute("CREATE TABLE IF NOT EXISTS covid.allcountries (country text PRIMARY KEY, deathlast14 float, confirmedlast14 float);")

    // make a connection to Kafka and read (key, value) pairs from it
    val kafkaConf = Map(
      "metadata.broker.list" -> "localhost:9092",
      "zookeeper.connect" -> "localhost:2181",
      "group.id" -> "kafka-spark-streaming",
      "zookeeper.connection.timeout.ms" -> "1000"
    )

    val sc =  new SparkConf().setAppName("FinalProject").setMaster("local");
    val ssc = new StreamingContext(sc, Seconds(1))
    ssc.checkpoint("checkpointDirectory")
    val topicsSet = Set("AllCountries")

    val kafkaStream  = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder]( ssc, kafkaConf, topicsSet)

    val messages = kafkaStream.map { x =>
      val data = x._2.split(",")
      val dateString = data(3).split("-")
      val date = new GregorianCalendar(dateString(0).toInt, dateString(1).toInt -1, dateString(2).toInt).getTime()
      val today = new Date()
      val diff = (today.getTime() - date.getTime())/1000/60/60/24
      val covidData = CovidData(data(0).toInt, data(1).toInt, data(2).toInt, date, diff.toInt)
      (x._1, covidData)
    }

    //messages.foreachRDD { rdd =>
    //  rdd.collect.foreach(println)
    //}

    // measure the average value for each key in a stateful manner
    def mappingFunc(key: String, value: Option[CovidData], state: State[Array[Double]]): (String, Double, Double) = {
	    //val today = DateTime.now().minusDays(14)
      val today = new Date()

      val newValue = value.getOrElse(CovidData(0, 0, 0, new GregorianCalendar(1995, 1, 1).getTime(), 1000))
      val valDate = newValue.date
      val newConfirmed = newValue.confirmed
      val newDeaths = newValue.deaths
      val diff = newValue.last14

      val states = state.getOption.getOrElse(Array(0.0, 0.0, 0.0, 0.0))
      var deathsTotal = states(0)
      var deathsBeforeLast14 = states(1)
      var confirmedTotal = states(2)
      var confirmedBeforeLast14 = states(3)

      if(diff > 14) {
        deathsBeforeLast14 = Math.max(deathsBeforeLast14, newDeaths)
        confirmedBeforeLast14 = Math.max(confirmedBeforeLast14, newConfirmed)
      }

      deathsTotal = Math.max(deathsTotal, newDeaths)
      confirmedTotal = Math.max(confirmedTotal, newConfirmed)

      val deathLast14 = deathsTotal-deathsBeforeLast14
      val confirmedLast14 = confirmedTotal - confirmedBeforeLast14

      state.update(Array(deathsTotal, deathsBeforeLast14, confirmedTotal, confirmedBeforeLast14))

      (key, deathLast14, confirmedLast14)
    }
    val stateDstream = messages.mapWithState(StateSpec.function(mappingFunc _))
    // store the result in Cassandra
    stateDstream.foreachRDD { rdd =>
      rdd.saveToCassandra("covid", "allcountries", SomeColumns("country", "deathlast14", "confirmedlast14"))
    }
    
    ssc.start()
    ssc.awaitTermination()
  }
}
