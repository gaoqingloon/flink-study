package com.lolo.flink.source

import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer

import scala.util.Random

object MyKafkaProducer {

  def main(args: Array[String]): Unit = {
    //连接Kafka的属性
    val props = new Properties()
    props.setProperty("bootstrap.servers", "hadoop101:9092,hadoop102:9092,hadoop103:9092")
    props.setProperty("key.serializer", classOf[StringSerializer].getName)
    props.setProperty("value.serializer", classOf[StringSerializer].getName)

    val producer = new KafkaProducer[String, String](props)
    val r = new Random()
    while (true) { //死循环生成键值对的数据
      val data = new ProducerRecord[String, String]("t_topic", "key" + r.nextInt(10), "value" + r.nextInt(100))
      producer.send(data)
      Thread.sleep(1000)
    }
    producer.close()
  }
}
