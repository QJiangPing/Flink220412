package com.qjp.day2

import org.apache.flink.streaming.api.scala._


object KeyStreamExample {
  def main(args: Array[String]): Unit = {
    val env : StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val stream : DataStream[SensorReading] = env
      .addSource(new SensorSource)
      .filter(_.id.equals("sensor_1"))

    //泛型变成了两个，第一个数输入类型，第二个是key的类型
    val keyed : KeyedStream[SensorReading, String] = stream.keyBy(_.id)

    //使用第三个字段来做滚动聚合，求每个传感器流上的最小温度值
    //内部会保留一个最小值状态变量，用来保存到来温度的最小值，来的温度一旦更新了温度最小值变量，原来的就会被抛弃
    keyed.min(2).print()

    //reduce也会保存一个变量
    //[r1,r2,r3,...]
    //[r1,r2] => [r,r3] =>[r,r4].....
    //r = 前两个元素归并的结果
    keyed.reduce((r1,r2) => SensorReading(r1.id,0,r1.temperature.min(r2.temperature))).print()

    env.execute()
  }
}
