package com.fst.kafka

import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

import scala.io.Source

object Demo1StudentToKafka {

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



    //读取学生表
    Source
      .fromFile("data/students.txt")
      .getLines()
      .foreach(student => {

        //将用一个班级的学生打入同一个分区
        val clazz: String = student.split(",")(4)
        val partition: Int = math.abs(clazz.hashCode) % 2



        //将数据发送到kafka

        //kafka-topics.sh --create --zookeeper master:2181 --replication-factor 1 --partitions 2 --topic student2
        val record = new ProducerRecord[String, String]("student2", partition, null, student)

        producer.send(record)
        producer.flush()


      })



    //关闭链接
    producer.close()

  }

}
