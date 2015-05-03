/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.examples.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.storage.StorageLevel

import org.apache.spark.streaming._
import scala.collection.mutable
import scala.util.Random
import org.apache.spark.rdd.RDD

/**
 * Counts words in UTF8 encoded, '\n' delimited text received from the network every second.
 *
 * Usage: NetworkWordCount <hostname> <port>
 * <hostname> and <port> describe the TCP server that Spark Streaming would connect to receive data.
 *
 * To run this on your local machine, you need to first run a Netcat server
 *    `$ nc -lk 9999`
 * and then run the example
 *    `$ bin/run-example org.apache.spark.examples.streaming.NetworkWordCount localhost 9999`
 */
object Spark7326 {
  def main(args: Array[String]) {

    StreamingExamples.setStreamingLogLevels()

    val Array(batchInterval) = Array("1234")
    val sparkConf = new SparkConf()
      .setAppName("Spark7326")
      .setMaster("local[2]")
      //.set("spark.mesos.coarse", "true")
    val ssc =  new StreamingContext(sparkConf, Milliseconds(batchInterval.toInt))
    ssc.checkpoint("checkpoint")

    def createRDD(i: Int) : RDD[(String, Int)] = {

      val count = 1000
      val rawLogs = (1 to count).map{ _ =>
        val word = "word" + Random.nextInt.abs % 5
        (word, 1)
      }
      ssc.sparkContext.parallelize(rawLogs)

    }

    val rddQueue = mutable.Queue[RDD[(String, Int)]]()
    val rawLogStream = ssc.queueStream(rddQueue)

    (1 to 300) foreach { i =>
      rddQueue.enqueue(createRDD(i))
    }


    val l1 = rawLogStream.window(Milliseconds(batchInterval.toInt) * 5, Milliseconds(batchInterval.toInt) * 5).reduceByKey(_ + _)

    val l2 = l1.window(Milliseconds(batchInterval.toInt) * 15, Milliseconds(batchInterval.toInt) * 15).reduceByKey(_ + _)

    val l3 = l2.window(Milliseconds(batchInterval.toInt) * 30, Milliseconds(batchInterval.toInt) * 30).reduceByKey(_ + _)

    l1.print()
    l2.print()
    l3.print()


    ssc.start()

    ssc.awaitTermination()
  }
}
