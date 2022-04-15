package com.qjp.day2

import org.apache.flink.streaming.api.functions.co.CoMapFunction
import org.apache.flink.streaming.api.scala._

object CoMapExample {

  case class Mapped(id: String,count: Int)

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    println(env.getParallelism)

    val weight :DataStream[(String,Int)] = env.fromElements(
      ("zuoyuan",130),
      ("baiyuan",100)
    )

    val age :DataStream[(String,Int)]  = env.fromElements(
      ("zuoyuan",33),
      ("baiyuan",45)
    )
    //直接connect两条流没有意义，
    //必须把相同的key的流connect在一起处理
    val connected : ConnectedStreams[(String,Int),(String,Int)] = weight
      .keyBy(_._1)
      .connect(age.keyBy(_._1))

    val printed :DataStream[String] = connected.map(new MyCoMapFunction())

    printed.print()

    env.execute()
  }

  class MyCoMapFunction extends CoMapFunction[(String,Int),(String,Int),String]{
    override def map1(in1: (String, Int)): String = {
      in1._1 + "的体重是" + in1._2 + "斤"
    }

    override def map2(in2: (String, Int)): String = {
      in2._1 + "的年龄是" + in2._2 + "岁"
    }
  }
}
