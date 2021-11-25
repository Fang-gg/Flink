package com.fst.transformation

import org.apache.flink.streaming.api.scala._

object Demo8Union {
  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    val ds1: DataStream[Int] = env.fromCollection(List(1,2,3,4,5,6))

    val ds2: DataStream[Int] = env.fromCollection(List(4,5,6,7,8,9))

    /**
      * 合并DataStream 类型要一致
      *
      */
    val unionDS: DataStream[Int] = ds1.union(ds2)

    unionDS.print()


    env.execute()

  }

}
