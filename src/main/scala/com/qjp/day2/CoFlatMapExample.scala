package com.qjp.day2

import org.apache.flink.streaming.api.functions.co.CoFlatMapFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

object CoFlatMapExample {

  case class Mapped(id: String,count: Int)

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val weight :DataStream[(String,Int)] = env.fromElements(
      ("zuoyuan",130),
      ("baiyuan",100)
    )

    val age :DataStream[(String,Int)]  = env.fromElements(
      ("zuoyuan",33),
      ("baiyuan",45),
      ("lingwai",45)
    )
    //直接connect两条流没有意义，
    //必须把相同的key的流connect在一起处理
    val connected : ConnectedStreams[(String,Int),(String,Int)] = weight
      .keyBy(_._1)
      .connect(age.keyBy(_._1))

    val printed :DataStream[String] = connected.flatMap(new MyCoFlatMapFunction())

    printed.print()

    env.execute()
  }

  class MyCoFlatMapFunction extends CoFlatMapFunction[(String,Int),(String,Int),String]{
    //实现第一条流打印两次，第二条流打印一次
    override def flatMap1(in1: (String, Int), collector: Collector[String]): Unit = {
      collector.collect(in1._1 + "的体重是" + in1._2 + "斤")
      collector.collect(in1._1 + "的体重是" + in1._2 + "斤")
    }

    override def flatMap2(in2: (String, Int), collector: Collector[String]): Unit = {
      collector.collect(in2._1 + "的年龄是" + in2._2 + "岁")
    }
  }
}
