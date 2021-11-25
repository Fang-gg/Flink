package com.fst.source

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.source.{RichSourceFunction, SourceFunction}
import org.apache.flink.streaming.api.scala._

import java.sql.{Connection, DriverManager, PreparedStatement, ResultSet}

object Demo2MysqlSource {
  def main(args: Array[String]): Unit = {
    val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    environment.setParallelism(2)

    // 使用自定义source
    val myDS: DataStream[(String, String, String, Int)] = environment.addSource(new MysqlSource)

    myDS.print()

    environment.execute()
  }
}

/**
 * 自定义读取mysql  ----有界流
 * SourceFunction ----单一的source，run方法只会执行一次
 * ParallelSourceFunction  ----并行的source，有多少个并行度就会有多少个source
 * RichSourceFunction  多了open和close方法
 * RichParallelSourceFunction
 */
class MysqlSource extends RichSourceFunction[(String, String, String, Int)] {

  var con: Connection = _
  /**
   *
   * open 方法会再run方法之前执行
   *
   * @param parameters flink 配置文件对象
   */
  override def open(parameters: Configuration): Unit = {
    //加载驱动
    Class.forName("com.mysql.jdbc.Driver")

    //1、建立链接
    con = DriverManager.getConnection("jdbc:mysql://master:3306/tour", "root", "123456")

  }

  /**
   * 在run方法之后执行
   *
   */
  override def close(): Unit = {
    //关闭链接
    con.close()
  }

  override def run(ctx: SourceFunction.SourceContext[(String, String, String, Int)]): Unit = {

    //查询数据
    val stat: PreparedStatement = con.prepareStatement("select * from usertag limit 2")

    val resultSet: ResultSet = stat.executeQuery()

    //解析数据
    while (resultSet.next()) {

      val mdn: String = resultSet.getString("mdn")
      val name: String = resultSet.getString("name")
      val gender: String = resultSet.getString("gender")
      val age: Int = resultSet.getInt("age")


      //将数据发送到下游
      ctx.collect((mdn, name, gender, age))
    }
  }

  override def cancel(): Unit = {

  }
}
