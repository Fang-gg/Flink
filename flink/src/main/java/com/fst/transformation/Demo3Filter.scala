package com.fst.transformation

import org.apache.flink.api.common.functions.FilterFunction
import org.apache.flink.streaming.api.scala._

object Demo3Filter {


  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment


    /**
      * flink的算子不是懒执行
      *
      */

    val studentDS: DataStream[String] = env.readTextFile("data/students.txt")

    val filterDS: DataStream[String] = studentDS.filter(new FilterFunction[String] {
      override def filter(stu: String): Boolean = {
        println("filter")
        stu.split(",")(3) == "男"
      }
    })

    filterDS.print()

    env.execute()
  }

}
