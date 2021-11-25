package com.fst.sink

import org.apache.flink.streaming.api.functions.sink.SinkFunction
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}

object Demo1Sink {


  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    val studentDS: DataStream[String] = env.readTextFile("data/students.txt")

    //    studentDS.print()

    /**
      * 自定义sink
      *
      */

    studentDS.addSink(new MySink)

    env.execute()
  }

}

class MySink extends SinkFunction[String] {
  /**
    * invoke ： 每一条数据都会执行一次
    *
    * @param line    数据
    * @param context 上下文对象
    */
  override def invoke(line: String, context: SinkFunction.Context[_]): Unit = {
    println(line)
  }
}
