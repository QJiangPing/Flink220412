package com.qjp.day3

import com.qjp.day2.{SensorReading, SensorSource}
import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

// 增量聚合
// 求平均温度
object AvgTempByAggregateWindowFunctionExample {

  case class AvgInfo(id: String, avg: Double, windowsStart: Long, windowEnd: Long)

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val stream : DataStream[SensorReading] = env.addSource(new SensorSource)

    stream.keyBy(_.id)
      .timeWindow(Time.seconds(5))
      .aggregate(new AvgTempAgg)
      .print()

    env.execute()
  }

  //增量聚合
  //第一个泛型：流中元素的类型
  //第二个泛型：累加器的类型，元组（传感器id，来了多少条温度读书，来的温度读书总和是多少）
  //第三个泛型：增量聚合函数的输出类型，是一个元组（传感器id，窗口温度平均值）
  class AvgTempAgg extends AggregateFunction[SensorReading, (String, Long, Double), (String, Double)]{
    //累加器1：创建一个空的累加器
    override def createAccumulator(): (String, Long, Double) = ("", 0L, 0.0)

    //累加器2：聚合逻辑是什么
    override def add(in: SensorReading, acc: (String, Long, Double)): (String, Long, Double) = {
      //值1：传感器id, 值2：来了多少条温度值，值3：来一条就把温度累加起来
      (in.id, acc._2 + 1, acc._3 + in.temperature)
    }

    //两个累加器合并的逻辑是什么
    override def merge(acc: (String, Long, Double), b: (String, Long, Double)): (String, Long, Double) = {
      (acc._1, acc._2 + b._2, acc._3 + b._3)
    }

    //窗口闭合时，输出的结果是什么
    override def getResult(acc: (String, Long, Double)): (String, Double) = {
      //计算平均值 = 总温度值 / 条数
      (acc._1, acc._3 / acc._2)
    }
  }
}
