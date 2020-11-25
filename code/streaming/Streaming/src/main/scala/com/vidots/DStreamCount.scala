package com.vidots

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.{StringDeserializer, StringSerializer}
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import com.github.benfradet.spark.kafka.writer._
import org.apache.kafka.clients.producer.ProducerRecord
import org.json4s.jackson.Serialization.write
import org.json4s.DefaultFormats

import scala.util.parsing.json.JSON

case class Stock(name: String, stockUpAndDrop: Double, currMonth: String, index: String)

// bin/spark-submit --class com.vidots.DStreamCount --master yarn --deploy-mode cluster ~/spark-jars/Streaming-1.0-SNAPSHOT.jar

object DStreamCount {
  def main(args: Array[String]): Unit = {
    // 取出每个月的前十名
    val topNum = 10
    val intervals = 10
    val debug = true

    // 定义Kafka相关参数
    val bootsrapServers = "hadoop-node-1:9092,hadoop-node-2:9092,hadoop-node-3:9092"
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> bootsrapServers,
      "key.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "stocker-group",
      "value.deserializer" -> classOf[StringDeserializer],
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )
    // 写入Kafka时的参数
    val writer_topic_times = "stock-analysis_times-topic"
    val writer_topic_trend  = "stock-analysis_trend-topic"

    val kafkaProducerConfig = Map(
      "bootstrap.servers" -> bootsrapServers,
      "key.serializer" -> classOf[StringSerializer].getName,
      "value.serializer" -> classOf[StringSerializer].getName
    )
    val conf = new SparkConf().setAppName("dstream")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    val ssc = new StreamingContext(sc, Seconds(intervals))
    // 从Kafka中读取数据
    val stream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream[String, String](ssc, PreferConsistent, Subscribe[String, String](Array("streamingInput20201116"), kafkaParams))
    val result: DStream[(String, String)] = stream.map(record => (record.key, record.value))
    val items: DStream[String] = result.map(_._2)

    // 将数据从JSON字符串形式转化为Stock类形式
    val stocks: DStream[Stock] = items.map(str => {
      val m = JSON.parseFull(str) match {
        case Some(map:Map[String,Any]) => map
        case _ => Map("name"->"", "stockUpAndDrop"-> 0.0, "currMonth"->"", "index"->"")
      }
      Stock(m("name").toString, m("stockUpAndDrop").asInstanceOf[Double], m("currMonth").toString, m("index").toString)
    })
    val vertedTimes: DStream[((String, String), Int)] = stocks.map(stock => {
      ((stock.name, stock.currMonth), 1)
    }).reduceByKey(_ + _)
    val vertedTrend: DStream[((String, String), Double)] = stocks.map(stock => {
      ((stock.name, stock.currMonth), stock.stockUpAndDrop)
    }).reduceByKey((left, right) => {
      (1 + left / 100) * (1 + right / 100)
    })
    // 对每个月的数据进行聚合(以股票名称和所在的月份)
    val topTimes: DStream[((String, String), Int)] = vertedTimes.transform(rdd => {
      val list: Array[((String, String), Int)] = rdd.sortBy(_._2, false).take(topNum)
      val result = rdd.filter(item => {
        list.contains(item)
      })
      result
    })
    val topTrend = vertedTrend.transform(rdd => {
      val list: Array[((String, String), Double)] = rdd.sortBy(_._2, false).take(topNum)
      val result = rdd.filter(item => {
        list.contains(item)
      })
      result
    })

    if (debug) {
      // 在本地开发最好，开启
      topTimes.print()
      topTrend.print()
    }

    topTimes.writeToKafka(kafkaProducerConfig,
      item => {
        val msgTime = Map("name"->item._1._1, "month"->item._1._2, "total"->item._2)
        implicit val formats = DefaultFormats
        val timeJson = write(msgTime)
        new ProducerRecord[String, String](writer_topic_times, timeJson)
      })

    topTrend.writeToKafka(kafkaProducerConfig,
      item => {
        val msgTrend = Map("name"->item._1._1, "month"->item._1._2, "trend"->item._2)
        implicit val formats = DefaultFormats
        val trendJson = write(msgTrend)
        new ProducerRecord[String, String](writer_topic_trend, trendJson)
      })

    ssc.start()
    ssc.awaitTermination()
  }
}
