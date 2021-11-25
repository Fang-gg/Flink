package com.fst.core

import org.apache.flink.streaming.api.scala._

object Demo1WordCount {
  def main(args: Array[String]): Unit = {

    /**
     * 创建Flink的环境
     */
    val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    // 设置并行度
    environment.setParallelism(2)

    // 读取socket数据
    // nc -lk 8888

    val linesDS: DataStream[String] = environment.socketTextStream("master", 8888)

    // 将单词拆分出来
    val wordDS: DataStream[String] = linesDS.flatMap(line => line.split(","))

    // 转换成KV格式
    val kvDS: DataStream[(String, Int)] = wordDS.map(word => (word, 1))

    // 按照单词分组
    val keyByDS: KeyedStream[(String, Int), ((String, Int), Int)] = kvDS.keyBy(word => (word, 1))

    // 统计单词的数量
    /**
     * sum算子内部是有状态计算，累加统计
     */
    val countDS: DataStream[(String, Int)] = keyByDS.sum(1)

    // 打印结果
    countDS.print()

    // 启动flink
    environment.execute()
  }
}
