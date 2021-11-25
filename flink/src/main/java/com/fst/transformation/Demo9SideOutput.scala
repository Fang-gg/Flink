package com.fst.transformation

import org.apache.flink.api.common.functions.FlatMapFunction
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

object Demo9SideOutput {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment


    val studentsDS: DataStream[String] = env.readTextFile("data/students.txt")

    /**
      * 将性别为男和性别为女学生单独拿出来
      *
      */

    val nan: OutputTag[String] = OutputTag[String]("男")
    val nv: OutputTag[String] = OutputTag[String]("女")


    val processDS: DataStream[String] = studentsDS.process(new ProcessFunction[String, String] {
      override def processElement(line: String, ctx: ProcessFunction[String, String]#Context, out: Collector[String]): Unit = {


        val gender: String = line.split(",")(3)

        gender match {
          //旁路输出
          case "男" => ctx.output(nan, line)
          case "女" => ctx.output(nv, line)
        }
      }
    })


    //获取旁路输出的DataStream

    val nanDS: DataStream[String] = processDS.getSideOutput(nan)
    val nvDS: DataStream[String] = processDS.getSideOutput(nv)


    nvDS.print()

    env.execute()

  }

}
