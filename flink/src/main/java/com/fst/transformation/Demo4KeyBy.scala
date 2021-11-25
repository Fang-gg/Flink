package com.fst.transformation

import org.apache.flink.api.java.functions.KeySelector
import org.apache.flink.streaming.api.scala._

object Demo4KeyBy {

  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    env.setParallelism(3)
    val linesDS: DataStream[String] = env.socketTextStream("master", 8888)


    /**
      * keyBy 将相同的key发送到同一个task中
      *
      */
    val keybyDS: KeyedStream[String, String] = linesDS.keyBy(new KeySelector[String, String] {
      override def getKey(line: String): String = {
        line
      }
    })

    keybyDS.print()


    env.execute()


  }

}
