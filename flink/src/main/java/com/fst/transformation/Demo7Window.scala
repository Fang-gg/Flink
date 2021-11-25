package com.fst.transformation

import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

object Demo7Window {
  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    val linesDS: DataStream[String] = env.socketTextStream("master", 8888)

    /**
      * 每个5秒统计一次单词的数量
      *
      */


    val kvDS: DataStream[(String, Int)] = linesDS.flatMap(_.split(",")).map((_, 1))

    val countDS: DataStream[(String, Int)] = kvDS
      .keyBy(_._1)
      //5秒一个窗口
      .timeWindow(Time.seconds(5))
      .sum(1)

    countDS.print()

    env.execute()

  }

}
