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
import java.time.LocalDate;
import java.lang.Math;
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord, ProducerConfig}
import scala.util.Random
import org.joda.time.DateTime
import org.apache.spark.sql.cassandra._
import com.datastax.spark.connector._
import com.datastax.driver.core.{Session, Cluster, Host, Metadata}
import com.datastax.spark.connector.streaming._
import java.time.temporal.ChronoUnit
import scala.collection.mutable.ArrayBuffer

import org.apache.spark.streaming.{Seconds, StreamingContext}



object KafkaSparkAllCountries {
  def main(args: Array[String]) {

    case class CountryCases(confirmed: Int, active: Int, deaths: Int, last14: Int, date: String)
    
    val cluster = Cluster.builder().addContactPoint("127.0.0.1").build()
    val session = cluster.connect()

    // connect to Cassandra and make a keyspace and table
    session.execute("CREATE KEYSPACE IF NOT EXISTS covid WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };")
    //session.execute("CREATE TABLE IF NOT EXISTS covid.countrycases (country text PRIMARY KEY, confirmed float, active float, deaths float, confirmedlast14 float, deathlast14 float, date text);")
    session.execute("CREATE TABLE IF NOT EXISTS covid.countrycases (country text, confirmed float, active float, deaths float, confirmedtoday float, deathstoday float, confirmedlast14 float, deathlast14 float, date text, PRIMARY KEY(country, date));")


    // make a connection to Kafka and read (key, value) pairs from it
    val kafkaConf = Map(
      "metadata.broker.list" -> "localhost:9092",
      "zookeeper.connect" -> "localhost:2181",
      "group.id" -> "kafka-spark-streaming",
      "zookeeper.connection.timeout.ms" -> "1000"
    )

    val conf =  new SparkConf().setAppName("CountryCases").setMaster("local");
    
    val ssc = new StreamingContext(conf, Seconds(1))
    val sc = ssc.sparkContext
    sc.setLogLevel("ERROR")
    ssc.checkpoint("checkpointDirectory")
    val topicsSet = Set("CountryCases")

    val kafkaStream  = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder]( ssc, kafkaConf, topicsSet)

    val messages = kafkaStream.map { x =>
      val data = x._2.split(",")
      // val dateString = data(3).split("-")
      // val date = new GregorianCalendar(dateString(0).toInt, dateString(1).toInt -1, dateString(2).toInt).getTime()
      // val diff = (today.getTime() - date.getTime())/1000/60/60/24
      val date = LocalDate.parse(data(3)); 
      val today = LocalDate.now()
      val diff = date.until(today, ChronoUnit.DAYS)
      val covidData = CountryCases(data(0).toInt, data(1).toInt, data(2).toInt, diff.toInt, data(3))
      (x._1, covidData)
    }

    // measure the average value for each key in a stateful manner
    /*def mappingFunc(key: String, value: Option[CountryCases], state: State[Array[Double]]): (String, Double, Double, Double, Double, Double, String) = {
      val newValue = value.getOrElse(CountryCases(0, 0, 0, 1000))
      val newConfirmed = newValue.confirmed
      val newActive = newValue.active
      val newDeaths = newValue.deaths
      val diff = newValue.last14

      val states = state.getOption.getOrElse(Array(10000.0, 0.0, 0.0, 0.0, 100000.0, 0.0, 0.0))
     
      var newestDate = states(0)
      var confirmedTotal = states(1)
      var activeTotal = states(2)
      var deathsTotal = states(3)

      var newestDateBeforeLast14 = states(4)
      var confirmedBeforeLast14 = states(5)
      var deathsBeforeLast14 = states(6)

      if(diff < newestDateBeforeLast14 && diff > 14) {
        confirmedBeforeLast14 = newConfirmed
        deathsBeforeLast14 = newDeaths
        newestDateBeforeLast14 = diff
      } 
      if(diff < newestDate) {
        confirmedTotal = newConfirmed
        activeTotal = newActive
        deathsTotal = newDeaths   
        newestDate = diff      
      }
      state.update(Array(newestDate, confirmedTotal, activeTotal, deathsTotal, newestDateBeforeLast14, confirmedBeforeLast14, deathsBeforeLast14))

      val confirmedLast14 = confirmedTotal - confirmedBeforeLast14
      val deathLast14 = deathsTotal-deathsBeforeLast14

      (key, confirmedTotal, activeTotal, deathsTotal, confirmedLast14, deathLast14, LocalDate.now().minusDays(newestDate.toLong).toString())
    }*/

    def mappingFunc(key: String, value: Option[CountryCases], state: State[ArrayBuffer[ArrayBuffer[Double]]]): (String, Int, Int, Int, Double, Double, Double, Double, String) = {
      val newValue = value.getOrElse(CountryCases(0, 0, 0, 0, ""))
      val newConfirmed = newValue.confirmed
      val newDeaths = newValue.deaths

      val states = state.getOption.getOrElse(
        ArrayBuffer(
          ArrayBuffer(0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0),
          ArrayBuffer(0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0)
        )
      )
     
      var confirmedArray = states(0)
      var deathArray = states(1)


      confirmedArray.remove(0)
      confirmedArray += newConfirmed
      deathArray.remove(0)
      deathArray += newDeaths

      if(confirmedArray(confirmedArray.length-2) > confirmedArray(confirmedArray.length-1)) {
        confirmedArray(confirmedArray.length-2) = confirmedArray(confirmedArray.length-1)
      }
      if(deathArray(deathArray.length-2) > deathArray(deathArray.length-1)) {
        deathArray(deathArray.length-2) = deathArray(deathArray.length-1)
      }
      
      state.update(ArrayBuffer(confirmedArray, deathArray))

      var confirmedLast14 = 0.0
      var deathLast14 = 0.0

      if(confirmedArray.length < 14) {
        confirmedLast14 = confirmedArray.last
        deathLast14 = deathArray.last
      } else {
        confirmedLast14 = confirmedArray.last - confirmedArray(0)
        deathLast14 = deathArray.last - deathArray(0)
      }

      val confirmedToday =  confirmedArray.last - confirmedArray(confirmedArray.length-2)
      val deathsToday = deathArray.last - deathArray(deathArray.length-2) 

      (key, newValue.confirmed, newValue.active, newValue.deaths, confirmedLast14, confirmedToday, deathLast14, deathsToday, newValue.date)
    }
    val stateDstream = messages.mapWithState(StateSpec.function(mappingFunc _))
    
    /*stateDstream.foreachRDD { rdd =>
      rdd.collect.foreach(println)
    }*/
    // store the result in Cassandra
    stateDstream.foreachRDD { rdd =>
      rdd.saveToCassandra("covid", "countrycases", SomeColumns("country", "confirmed", "active", "deaths", "confirmedlast14", "confirmedtoday", "deathlast14", "deathstoday", "date"))
      //rdd.saveToCassandra("covid", "countrycases", SomeColumns("country", "confirmed", "active", "deaths", "confirmedlast14", "deathlast14", "date"))
    }
    ssc.start()
    ssc.awaitTermination()
  }
}
