package com.fst.transformation

import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.streaming.api.scala._

object Demo1Map {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    val linesDS: DataStream[String] = env.socketTextStream("master", 8888)

    /**
      * map函数，
      * 传入一个函数
      * 传入接口的实现类 -- MapFunction
      *
      */
    val mapDS: DataStream[String] = linesDS.map(new MapFunction[String, String] {
      override def map(line: String): String = {
        line + "java"
      }
    })


    mapDS.print()


    env.execute()
  }

}
