package com.qjp.day2

import org.apache.flink.api.common.functions.FlatMapFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

//FlatMap可以实现map和filter算子的功能，用flatmap处理根据输入事件的颜色来做不同的处理
//例如，输入事件是白色，则直接输出，输入事件是黑色，加倍输出，输入元素是灰色，过滤掉
object FlatMapExample {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val stream = env.fromElements("white",
    "black",
    "gray")

    stream.flatMap(new MyFlatMapFunction).print()

    env.execute()
  }

  //flatMap算子，针对每一个元素，生成0个，1个，或者多个数据
  class MyFlatMapFunction extends FlatMapFunction[String, String]{
    override def flatMap(t: String, collector: Collector[String]): Unit = {
      if (t.equals("white")){
        collector.collect(t)
      }else if (t.equals("black")){
        collector.collect(t)
        collector.collect(t)
      }
    }
  }
}
