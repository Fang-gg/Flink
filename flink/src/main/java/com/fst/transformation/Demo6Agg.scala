package com.fst.transformation

import org.apache.flink.streaming.api.scala._

object Demo6Agg {

  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    val studentDS: DataStream[String] = env.readTextFile("data/students.txt")


    val stuDS: DataStream[Student] = studentDS.map(line => {
      val split: Array[String] = line.split(",")
      Student(split(0), split(1), split(2).toInt, split(3), split(4))
    })


    stuDS
      .keyBy(_.clazz)
      .sum("age")
    // .print()


    /**
      * max 和 maxBy 之间的区别在于 max 返回流中的最大值，但 maxBy 返回具有最大值的键，
      */
    stuDS
      .keyBy(_.clazz)
      .maxBy("age")
      .print()


    env.execute()
  }

  case class Student(id: String, name: String, age: Int, gender: String, clazz: String)

}
