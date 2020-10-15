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
import java.util.{Date, Properties}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord, ProducerConfig}
import scala.util.Random

import org.apache.spark.sql.cassandra._
import com.datastax.spark.connector._
import com.datastax.driver.core.{Session, Cluster, Host, Metadata}
import com.datastax.spark.connector.streaming._

import org.apache.spark.streaming.{Seconds, StreamingContext}

object KafkaSpark {
  def main(args: Array[String]) {
    // connect to Cassandra and make a keyspace and table as explained in the document
    val cluster = Cluster.builder().addContactPoint("127.0.0.1").build()
    val session = cluster.connect()

    //session.execute("CREATE KEYSPACE IF NOT EXISTS avg_space WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };")
    //session.execute("CREATE TABLE IF NOT EXISTS avg_space.avg (word text PRIMARY KEY, count float);")

    // make a connection to Kafka and read (key, value) pairs from it
    val kafkaConf = Map(
      "metadata.broker.list" -> "localhost:9092",
      "zookeeper.connect" -> "localhost:2181",
      "group.id" -> "kafka-spark-streaming",
      "zookeeper.connection.timeout.ms" -> "1000"
    )

    val sc =  new SparkConf().setAppName("FinalProject").setMaster("local");
    val ssc = new StreamingContext(sc, Seconds(5))
    ssc.checkpoint("checkpointDirectory")
    val topicsSet = Set("AllCountries")

    val kafkaStream  = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder]( ssc, kafkaConf, topicsSet)
    
    val messages = kafkaStream.map(x => x._2)

    messages.foreachRDD { rdd =>
      rdd.take(10).foreach(println)
    }
  
    //val words = messages.map(message => (message.split(","))).map(arr => arr(0) -> arr(1))

    //val pairs = words.map(word => (word._1, word._2.toDouble))

    // measure the average value for each key in a stateful manner
    /*def mappingFunc(key: String, value: Option[Double], state: State[Array[Double]]): (String, Double) = {
	    val newValue = value.getOrElse(0.0)
      val states = state.getOption.getOrElse(Array(0.0, 0.0))
      val oldCount = states(0)
      val newCount = oldCount+1
      val oldValue = states(1)
      val sum = (oldValue*oldCount + newValue)/newCount
      state.update(Array(newCount, sum))
      (key, sum)
    }*/
    //val stateDstream = pairs.mapWithState(StateSpec.function(mappingFunc _))

    // store the result in Cassandra
    //stateDstream.foreachRDD { rdd =>
    //  rdd.saveToCassandra("avg_space", "avg", SomeColumns("word", "count"))
    //}
    
    ssc.start()
    ssc.awaitTermination()
  }
}
