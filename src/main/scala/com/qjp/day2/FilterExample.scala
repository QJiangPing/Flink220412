package com.qjp.day2

import org.apache.flink.api.common.functions.{FilterFunction, MapFunction}
import org.apache.flink.streaming.api.scala._

object FilterExample {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val stream = env.addSource(new SensorSource)

    //方式1，直接使用
    stream.filter(t => t.id.equals("sensor_1")).print()

    //方式2，继承，重写
    stream.filter(new MyFilterFunction).print()

    //方式3，匿名函数
    stream.filter(new FilterFunction[SensorReading] {
      override def filter(t: SensorReading): Boolean = t.id.equals("sensor_1")
    }).print()


    env.execute()
  }

  //filter算子的输入和输出类型是一样的，所以只有一个泛型SensorReading
  class MyFilterFunction extends FilterFunction[SensorReading]{
    override def filter(t: SensorReading): Boolean = t.id.equals("sensor_1")
  }
}
