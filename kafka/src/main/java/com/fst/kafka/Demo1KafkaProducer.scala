package com.fst.kafka

import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

object Demo1KafkaProducer {

  def main(args: Array[String]): Unit = {

    /**
      * 1、创建kfaka链接
      * 创建生产者
      *
      */

    val properties = new Properties()

    //指定kafka的broker的地址
    properties.setProperty("bootstrap.servers", "master:9092")
    //key和value序列化类
    properties.setProperty("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    properties.setProperty("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")


    //生产者
    val producer = new KafkaProducer[String, String](properties)


    //生产数据
    //topic 不存在会自动创建一个分区为1副本为1的topic
    val record = new ProducerRecord[String, String]("test1", "java")

    producer.send(record)

    //将数据刷到kafka中
    producer.flush()


    //关闭链接
    producer.close()

  }

}
