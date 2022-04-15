package com.qjp.day2

import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala._

object RichFunctionExample {

  case class Mapped(id: String,count: Int)

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val stream = env.fromElements("hello world")

    stream.map(new MyRichMap).print
    env.execute()
  }

  class MyRichMap extends RichMapFunction[String, String]{
    override def open(parameters: Configuration): Unit = {
      println("生命周期结束")
    }

    override def map(in: String): String = {
      val taskName = getRuntimeContext.getTaskName
      "任务的名字是" + taskName

    }

    override def close(): Unit = {
      println("生命周期结束")
    }
  }
}
