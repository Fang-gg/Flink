package com.fst.transformation

import org.apache.flink.api.common.functions.{FlatMapFunction, MapFunction, RichFlatMapFunction}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

object Demo2FlatMap {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    env.setParallelism(4)

    val linesDS: DataStream[String] = env.socketTextStream("master", 8888)

    /**
      * FlatMapFunction
      * RichFlatMapFunction -- 多了open和close方法， 可以做初始化操作
      *
      */


    val flatMapDS: DataStream[String] = linesDS.flatMap(new RichFlatMapFunction[String, String] {


      /**
        * open 方法在每一个task中只执行一次，在flatMap之前执行
        *
        */
      override def open(parameters: Configuration): Unit = {
        println("open......")
      }

      /** 在每一个task中只执行一次，在flatMap之后执行
        *
        */
      override def close(): Unit = {
        println("close......")
      }

      /**
        * flatMap函数，每一条数据执行一次
        *
        * @param line ： 一行数据
        * @param out  ; 用于将数据发送到下游
        */
      override def flatMap(line: String, out: Collector[String]): Unit = {

        line
          .split(",")
          .foreach(word => {

            //将数据发送到下游
            out.collect(word)
          })

      }
    })


    flatMapDS.print()


    env.execute()

  }

}
