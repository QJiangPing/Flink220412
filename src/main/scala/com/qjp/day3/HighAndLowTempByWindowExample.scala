package com.qjp.day3

import com.qjp.day2.{SensorReading, SensorSource}
import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

// 增量聚合 + 全窗口聚合函数
// 求平均温度
object HighAndLowTempByWindowExample {

  //样例类
  case class AvgInfo(id: String, min: Double, max: Double, windowsStart: Long, windowEnd: Long)

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val stream : DataStream[SensorReading] = env.addSource(new SensorSource)

    stream.keyBy(_.id)
      .timeWindow(Time.seconds(5))
      .aggregate(new HighAndLowAgg, new WindowResult)
      .print()

    env.execute()
  }

  class HighAndLowAgg extends AggregateFunction[SensorReading, (String, Double, Double),(String, Double, Double)]{
    //累加器1：创建一个空的累加器,
    // 最小温度的初始值是Double的最大值,最大温度的初始值是Double的最小值
    override def createAccumulator(): (String, Double, Double) = ("", Double.MaxValue, Double.MinValue )

    //累加器2：聚合逻辑是什么
    override def add(in: SensorReading, acc: (String, Double, Double)): (String, Double, Double) = {
      //值1：传感器id, 值2：来了多少条温度值，值3：来一条就把温度累加起来
      (in.id, in.temperature.min(acc._2), in.temperature.max(acc._2))
    }

    //两个累加器合并的逻辑是什么
    override def merge(a: (String, Double, Double), b: (String, Double, Double)): (String, Double, Double) = {
      (a._1, a._2.min(b._2), a._2.max(b._2))
    }

    //窗口闭合时，输出的结果是什么
    override def getResult(acc: (String, Double, Double)): (String, Double, Double) = acc
  }


  //注意,输入泛型是增量聚合函数的输出泛型
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
