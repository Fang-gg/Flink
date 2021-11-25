package com.fst.transformation

import org.apache.flink.api.common.functions.ReduceFunction
import org.apache.flink.streaming.api.scala._

object Demo5Reduce {

  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment


    val lineDS: DataStream[String] = env.socketTextStream("master", 8888)


    val kvDS: DataStream[(String, Int)] = lineDS.flatMap(_.split(",")).map((_, 1))

    val keyByDS: KeyedStream[(String, Int), String] = kvDS.keyBy(_._1)


    /**
      * reduce : keyBy之后对数据进行聚合
      *
      */
    //    val reduceDS: DataStream[(String, Int)] = keyByDS.reduce((x, y) => (x._1, x._2 + y._2))


    val reduceDS: DataStream[(String, Int)] = keyByDS.reduce(new ReduceFunction[(String, Int)] {
      override def reduce(value1: (String, Int), value2: (String, Int)): (String, Int) =
        (value1._1, value1._2 + value2._2)
    })


    reduceDS.print()


    env.execute()

  }

}
