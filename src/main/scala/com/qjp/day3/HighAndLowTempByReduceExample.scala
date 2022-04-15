package com.qjp.day3

import com.qjp.day2.{SensorReading, SensorSource}
import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

// reduce + 全窗口聚合函数
// 求平均温度
object HighAndLowTempByReduceExample {

  //样例类
  case class AvgInfo(id: String, min: Double, max: Double, windowsStart: Long, windowEnd: Long)

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val stream : DataStream[SensorReading] = env.addSource(new SensorSource)

    stream
      .map(r => (r.id, r.temperature, r.temperature))
      .keyBy(_._1)
      .timeWindow(Time.seconds(5))
      .reduce((r1: (String, Double, Double),r2 : (String, Double, Double)) => {
        (r1._1, r1._2.min(r2._2), r1._3.max(r2._3))
      }, new WindowResult)
      .print()

    env.execute()
  }


  //注意,输入泛型是上游函数的输出泛型
  class WindowResult extends ProcessWindowFunction[(String, Double, Double), AvgInfo, String, TimeWindow]{

    override def process(key: String, context: Context, elements: Iterable[(String, Double, Double)], out: Collector[AvgInfo]): Unit = {
      val minAndMax = elements.head
      //单位ms
      val windowsStart = context.window.getStart
      val windowEnd = context.window.getEnd

      //迭代器只有一个值，就是增量聚合函数发送过来的聚合结果值
      out.collect(AvgInfo(key, minAndMax._2, elements.head._3, windowsStart, windowEnd))

    }

  }
}
