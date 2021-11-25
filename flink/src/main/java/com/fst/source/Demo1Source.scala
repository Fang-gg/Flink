package com.fst.source

import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.scala._

object Demo1Source {
  def main(args: Array[String]): Unit = {

    /**
     * 创建Flink环境
     */

    val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    // 设置并行度
    environment.setParallelism(2)
    /**
     * 基于本地集合构建DataStream  ----有界流
     */

    val listDS: DataStream[Int] = environment.fromCollection(List(1, 2, 3, 4, 5, 6, 7, 8, 9))
//    listDS.print()

    /**
     * 基于文件构建DataStream     ----有界流
     */

    val studentDS: DataStream[String] = environment.readTextFile("data/students.txt")

    studentDS
      .map(line=>(line.split(",")(4),1))
      .keyBy(word=>(word,1))
      .sum(1)
//      .print()


    /**
     * 基于Socket构建DataStream   ----无界流
     */

//    val socketDS: DataStream[String] = environment.socketTextStream("master", 8888)
//    socketDS.print()

    /**
     * 自定义source，实现SourceFunction接口
     */

    val myDS: DataStream[Int] = environment.addSource(new MySource)

    myDS.print()

    // 启动flink
    environment.execute()
  }
}

    /**
     * 自定义source，实现SourceFunction接口
     * 实现run方法
     */
    class MySource extends SourceFunction[Int]{
      override def run(sourceContext: SourceFunction.SourceContext[Int]): Unit = {

        var i = 0
        while (true){
          // 将数据发送到下游
          sourceContext.collect(i)

          Thread.sleep(50)

          i += 1
        }
      }
      /**
       * 再任务取消的时候执行，用于回收资源
       *
       */
      override def cancel(): Unit = {}
    }
