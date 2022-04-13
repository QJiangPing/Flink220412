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

package com.qjp.day1
import org.apache.flink.streaming.api.scala._


object WordCountFromBatch {

  case class WordWithCount(word: String, count: Int)
  def main(args: Array[String]):Unit = {
    // set up the batch execution environment
    //val env = ExecutionEnvironment.getExecutionEnvironment
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val stream = env.fromElements(
      "hello world",
      "hello world",
      "hello world",
      "hello qjp"
    )

    val transformed = stream
      //用空格进行分割
      .flatMap(line => line.split("\\s"))
      .map(w => WordWithCount(w, 1))
      .keyBy(0)
      .sum(1)

    transformed.print()

    // execute program
    env.execute("Flink Batch Scala API Skeleton")
  }
}