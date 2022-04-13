package com.qjp.day1

import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

object WordCountFromSocket {

  case class WordWithCount(word: String, count: Int)

  def main(args: Array[String]): Unit = {
    //获取运行时环境
    // set up the streaming execution environment
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    //设置分区的数量为1
    env.setParallelism(1)

    //建立数据源
    //需要先启动 `nc -lk 9999`
    val stream = env.socketTextStream("localhost", 9999, '\n')

    //写对流的转换操作
    val transformed = stream
      //使用空格切分输入的字符串
      .flatMap(line => line.split("\\s"))
      //类似于MR中的map
      .map(w => WordWithCount(w, 1))
      //使用word字段进行分组，shuffle
      .keyBy(0)
      //开了一个5s的窗口
      .timeWindow(Time.seconds(5))
      //针对count字段进行累加操作，类似MR中的reduce
      .sum(1)

    //将计算结果输出
    transformed.print()

    // execute program
    env.execute("1111Flink Streaming Scala API Skeleton")
  }
}
