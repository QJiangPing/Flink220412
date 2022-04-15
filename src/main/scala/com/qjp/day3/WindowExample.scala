package com.qjp.day3

import com.qjp.day2.{SensorReading, SensorSource}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow

object WindowExample {

  case class Mapped(id: String,count: Int)

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val stream : DataStream[SensorReading] = env.addSource(new SensorSource)

    val keyedStream : KeyedStream[SensorReading,String] = stream
      .keyBy(_.id)

    //timeWindow()
    //接收一个参数时，表示滚动时间窗口，参数为滚动窗口的长度
    //接收两个参数时，表示滑动时间窗口，第一个参数是窗口长度：10秒，第二个参数是滑动距离：5秒
    val windowStream :WindowedStream[SensorReading, String, TimeWindow] = keyedStream
      .timeWindow(Time.seconds(10),Time.seconds(5))

    //会话窗口,设置一下超时时间就ok了
    //keyedStream.window(EventTimeSessionWindows.withGap(Time.minutes(5)))

    val printed : DataStream[SensorReading] = windowStream
      .reduce((r1,r2) => SensorReading(r1.id, 0 , r1.temperature.min(r2.temperature)))

    printed.print()
    env.execute()
  }

}
