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

import org.apache.flink.streaming.api.scala._


object ConsumeFromSensorSource {
  def main(args: Array[String]): Unit = {
    // set up the streaming execution environment
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    env.setParallelism(1)

    //调用addSource方法
    val stream = env.addSource(new SensorSource)

    stream.print()
    // execute program
    env.execute()
  }
}
