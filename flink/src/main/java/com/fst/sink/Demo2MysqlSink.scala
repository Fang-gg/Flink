package com.fst.sink

import java.sql.{Connection, DriverManager, PreparedStatement}

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.apache.flink.streaming.api.scala._

object Demo2MysqlSink {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    val studentDS: DataStream[String] = env.readTextFile("data/students.txt")

    studentDS.addSink(new MysqlSink)

    env.execute()
  }

}

class MysqlSink extends RichSinkFunction[String] {


  var con: Connection = _

  /**
    * 在invoke 之前执行，每一个task中只只一次
    *
    */
  override def open(parameters: Configuration): Unit = {
    println("创建链接")
    //加载驱动
    Class.forName("com.mysql.jdbc.Driver")

    //1、建立链接
    con = DriverManager.getConnection("jdbc:mysql://master:3306/tour?useUnicode=true&characterEncoding=utf-8", "root", "123456")

  }

  override def close(): Unit = {
    con.close()
  }

  /**
    * 每一条数据都会执行一次
    *
    */
  override def invoke(line: String, context: SinkFunction.Context[_]): Unit = {

    val split: Array[String] = line.split(",")


    val stat: PreparedStatement = con.prepareStatement("insert into student(id,name,age,gender,clazz) values(?,?,?,?,?)")

    stat.setString(1, split(0))
    stat.setString(2, split(1))
    stat.setInt(3, split(2).toInt)
    stat.setString(4, split(3))
    stat.setString(5, split(4))

    stat.execute()

  }
}