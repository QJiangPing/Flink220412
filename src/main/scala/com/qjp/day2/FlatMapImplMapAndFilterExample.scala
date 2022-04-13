package com.qjp.day2

import org.apache.flink.api.common.functions.FlatMapFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

//FlatMap可以实现map和filter算子的功能，用flatmap处理根据输入事件的颜色来做不同的处理
//例如，输入事件是白色，则直接输出，输入事件是黑色，加倍输出，输入元素是灰色，过滤掉
object FlatMapImplMapAndFilterExample {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val stream = env.addSource(new SensorSource)

    //实现map操作的功能
    stream.flatMap(new FlatMapFunction[SensorReading, String] {
      override def flatMap(t: SensorReading, collector: Collector[String]): Unit = {
        collector.collect(t.id)
      }
    }).print()

    //实现filter操作的功能
    stream.flatMap(new FlatMapFunction[SensorReading, SensorReading] {
      override def flatMap(t: SensorReading, collector: Collector[SensorReading]): Unit = {
        if (t.id.equals("sensor_3")){
          collector.collect(t)
        }
      }
    }).print()

    env.execute()
  }
}
