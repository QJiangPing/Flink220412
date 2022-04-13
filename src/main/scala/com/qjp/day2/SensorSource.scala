/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.qjp.day2

import org.apache.flink.streaming.api.functions.source.{RichParallelSourceFunction, SourceFunction}

import java.util.Calendar
import scala.util.Random

//泛型是SensorReading，表示产生的流中的时间类型是SensorReading
class SensorSource extends RichParallelSourceFunction[SensorReading] {
  //变量表示数据源是否正常运行，默认是运行状态
  var running: Boolean = true

  //上下文参数用来发出数据
  override def run(ctx: SourceFunction.SourceContext[SensorReading]): Unit = {
    val rand = new Random()
    //使用高斯噪声产生随机温度值
    var curFTemp = (1 to 10).map(
      i => ("sensor_" + i, (rand.nextGaussian() * 20))
    )

    //产生无限流数据
    while (running){
      curFTemp = curFTemp.map(
        t => (t._1, t._2 + (rand.nextGaussian() * 0.5))
      )

      //产生毫秒类型的时间戳
      val curTime = Calendar.getInstance().getTimeInMillis

      //使用ctx参数的collect方法发射传感器数据
      curFTemp.foreach(t => ctx.collect(SensorReading(t._1, curTime, t._2)))

      //每隔100毫秒发送一条传感器数据
      Thread.sleep(100)
    }
  }

  //定义当取消flink任务时，需要关闭数据源
  override def cancel(): Unit = running = false
}