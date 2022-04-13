package com.qjp.day2

import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.streaming.api.scala._
object MapExample {

  case class Mapped(id: String,count: Int)

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val stream = env.addSource(new SensorSource)

    //方式1
    val mapped1: DataStream[String] = stream.map(t => t.id)


    //方式2，继承，重写
    val mapped2: DataStream[String] = stream.map(new MyMapFuction)

    //方式3，匿名函数
    val mapped3: DataStream[String] = stream.map(new MapFunction[SensorReading, String] {
      override def map(t: SensorReading): String = t.id
    })


    mapped1.print()
    mapped2.print()
    mapped3.print()

    env.execute()
  }

  //T输入泛型：SensorReading，O输出泛型：String
  class MyMapFuction extends MapFunction[SensorReading, String]{
    override def map(value: SensorReading): String = value.id
  }
}
