package com.fst.kafka

import java.util
import java.util.Properties

import org.apache.kafka.clients.consumer.{ConsumerRecord, ConsumerRecords, KafkaConsumer}

object Demo3Comsumer {
  def main(args: Array[String]): Unit = {

    //1、创建消费者

    val properties = new Properties()

    //指定kafka的broker的地址
    properties.setProperty("bootstrap.servers", "master:9092")
    properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("group.id", "asdasdd")


    /**
      * earliest
      * 当各分区下有已提交的offset时，从提交的offset开始消费；无提交的offset时，从头开始消费
      * latest
      * 当各分区下有已提交的offset时，从提交的offset开始消费；无提交的offset时，消费新产生的该分区下的数据
      * none
      * topic各分区都存在已提交的offset时，从offset后开始消费；只要有一个分区不存在已提交的offset，则抛出异常
      *
      */


    //从最早读取数据
    properties.put("auto.offset.reset", "earliest")


    val consumer = new KafkaConsumer[String, String](properties)
    println("链接创建成功")

    //订阅topic
    val topics = new util.ArrayList[String]()
    topics.add("student2")
    consumer.subscribe(topics)


    while (true) {
      //消费数据
      val records: ConsumerRecords[String, String] = consumer.poll(1000)
      println("正在消费数据")

      //获取读到的所有数据
      val iterator: util.Iterator[ConsumerRecord[String, String]] = records.iterator()

      while (iterator.hasNext) {
        //获取一行数据
        val record: ConsumerRecord[String, String] = iterator.next()

        val topic: String = record.topic()
        val patition: Int = record.partition()
        val offset: Long = record.offset()
        val key: String = record.key()
        val value: String = record.value()
        //默认是系统时间
        val ts: Long = record.timestamp()
        println(s"$topic\t$patition\t$offset\t$key\t$value\t$ts")

      }

    }


    consumer.close()


  }

}
