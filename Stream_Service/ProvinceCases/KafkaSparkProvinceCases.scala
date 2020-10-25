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
import scala.collection.mutable.ArrayBuffer

import org.apache.spark.streaming.{Seconds, StreamingContext}



object KafkaSparkAllCountries {
  def main(args: Array[String]) {

    case class ProvinceData(country: String, lat: Double, lon: Double, confirmed: Int, active: Int, death: Int, weekOfYear: Int)
    val cluster = Cluster.builder().addContactPoint("127.0.0.1").build()
    val session = cluster.connect()

    // connect to Cassandra and make a keyspace and table
    session.execute("CREATE KEYSPACE IF NOT EXISTS covid WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };")
    session.execute("CREATE TABLE IF NOT EXISTS covid.provincecases (country text, province text, lat float, lon float, confirmed float, active float, death float, week float, PRIMARY KEY(week, lon, lat));")
    // make a connection to Kafka and read (key, value) pairs from it
    val kafkaConf = Map(
      "metadata.broker.list" -> "localhost:9092",
      "zookeeper.connect" -> "localhost:2181",
      "group.id" -> "kafka-spark-streaming",
      "zookeeper.connection.timeout.ms" -> "1000"
    )

    val conf =  new SparkConf().setAppName("ProvinceCases").setMaster("local");
    val ssc = new StreamingContext(conf, Seconds(1))
    val sc = ssc.sparkContext
    sc.setLogLevel("ERROR")
    ssc.checkpoint("checkpointDirectory")

    val topicsSet = Set("ProvinceCases")
    val kafkaStream  = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder]( ssc, kafkaConf, topicsSet)

    val messages = kafkaStream.map { x =>
      val country = x._1
      val data = x._2.split(",")
      var province = data(0)
      if (province == "") {
        province = country
      }
      val lat = data(1)
      val lon = data(2)
      val confirmed = data(3)
      val active = data(4)
      val death = data(5)
      val dateString = data(6).split("-")
      val date = new GregorianCalendar(dateString(0).toInt, dateString(1).toInt -1, dateString(2).toInt)
      val weekOfYear = date.get(Calendar.WEEK_OF_YEAR)
      val provinceData = ProvinceData(country, lat.toDouble, lon.toDouble, confirmed.toInt, active.toInt, death.toInt, weekOfYear)
      (province, provinceData)
    }

    messages.foreachRDD { rdd =>
      rdd.take(5).foreach(println)
    }

    def mappingFunc1(key: String, value: Option[ProvinceData], state: State[ArrayBuffer[Int]]): (String, ProvinceData) = {
      val newValue = value.getOrElse(ProvinceData("", 0.0, 0.0, 0, 0, 0, 0))

      val yesterdaysCases = state.getOption.getOrElse(ArrayBuffer(0, 0, 0))

      //var yesterdaysCases = states(0)
      //var yesterdaysCumilativeCases = states(1)
      //var dayBeforeCumilativeCases = states(2)

      //if (newValue.confirmed < yesterdaysCumilativeCases(0)) {
      //  yesterdaysCumilativeCases(0) = dayBeforeCumilativeCases(0)
      //}
      //if (newValue.death < yesterdaysCumilativeCases(1)) {
      //  yesterdaysCumilativeCases(2) = dayBeforeCumilativeCases(1)
      //  yesterdaysCases = 
      //}

      var confirmedToday = newValue.confirmed - yesterdaysCases(0)
      val activeToday = newValue.active - yesterdaysCases(1)
      var deathsToday = newValue.death - yesterdaysCases(2)

      if (confirmedToday < 0) { confirmedToday = 0}
      if (deathsToday < 0) { deathsToday = 0}

      state.update(ArrayBuffer(newValue.confirmed, newValue.active, newValue.death))

      val provinceData = ProvinceData(newValue.country, newValue.lat, newValue.lon, confirmedToday, activeToday, deathsToday, newValue.weekOfYear)
      val provinceWeek = key + "," + newValue.weekOfYear.toString
      (provinceWeek, provinceData)
    }
    val stateDstream1 = messages.mapWithState(StateSpec.function(mappingFunc1 _))

    def mappingFunc2(key: String, value: Option[ProvinceData], state: State[ArrayBuffer[ArrayBuffer[Int]]]): (String, String, Double, Double, Double, Double, Double, Int) = {
      val keys = key.split(",")
      val province = keys(0).toString
      val week = keys(1).toInt
      //ProvinceData(country: String, lat: Double, lon: Double, confirmed: Int, active: Int, death: Int, weekOfYear: Int)
      val newValue = value.getOrElse(ProvinceData("", 0.0, 0.0, 0, 0, 0, 0))

      val states = state.getOption.getOrElse(ArrayBuffer(ArrayBuffer(0), ArrayBuffer(0), ArrayBuffer(0)))

      var confirmedArray = states(0)
      var activeArray = states(1)
      var deathArray = states(2)

      confirmedArray += newValue.confirmed
      activeArray += newValue.active
      deathArray += newValue.death

      state.update(ArrayBuffer(confirmedArray, activeArray, deathArray))

      var newConfirmed = 0
      var newActive = 0
      var newDeaths = 0

      for(i <- 0 to confirmedArray.length-1) {
        newConfirmed += confirmedArray(i)
        newActive += activeArray(i)
        newDeaths += deathArray(i)
      }

      (newValue.country, province, newValue.lat, newValue.lon, newConfirmed, newActive, newDeaths, week)
    }
    val stateDstream2 = stateDstream1.mapWithState(StateSpec.function(mappingFunc2 _))

    // store the result in Cassandra
    stateDstream2.foreachRDD { rdd =>
      rdd.saveToCassandra("covid", "provincecases", SomeColumns("country", "province", "lat", "lon", "confirmed", "active", "death", "week"))
    }
    
    ssc.start()
    ssc.awaitTermination()
  }
}
