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

package org.apache.spark

import java.io.File
import scala.sys.process._

import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag


object Sort {
  def main(args: Array[String]): Unit = {
    val numParts = args(0).toInt



  }

  def readSlaves(): Array[String] = {
    scala.io.Source.fromFile("/root/spark-ec2/slaves").getLines().toArray
  }

  def gatherHostnames(sc: SparkContext): Array[String] = {
    sc.parallelize(1 to 1000, 1000).mapPartitions { iter =>
      val host = "hostname".!!
      Iterator(host.trim)
    }.collect().distinct.toArray
  }
}


object SortDataGenerator {

  var sc: SparkContext = null

  def main(args: Array[String]): Unit = {
    val sizeInTB = args(0).toInt
    val numParts = args(1).toInt
    sc = new SparkContext(new SparkConf())
  }

  def genSort(sizeInGB: Int, numParts: Int, numEbsVols: Int = 8): Unit = {

    val sizeInBytes = sizeInGB * 1000 * 1000 * 1000
    val numRecords = (sizeInBytes / 100).toLong
    val recordsPerPartition = math.ceil(numRecords.toDouble / numParts).toLong

    val hosts = Sort.gatherHostnames(sc)

    val output = new NodeLocalRDD[(String, Int, String, String)](sc, numParts, hosts) {
      override def compute(split: Partition, context: TaskContext) = {
        val part = split.index

        val start = recordsPerPartition * part
        val volIndex = part % numEbsVols

        val baseFolder = s"/vol$volIndex/sort-${sizeInGB}g"
        if (!new File(baseFolder).exists()) {
          new File(baseFolder).mkdirs()
        }

        val outputFile = s"$baseFolder/part$part"
        val cmd = s"/root/gensort/64/gensort -c -b$start -t1 $recordsPerPartition $outputFile"
        println(cmd)
        val result = cmd.!!
        val host = "hostname".!!
        println(s"$part\t$host\t$outputFile\t$result")
        Iterator((host, part, outputFile, result.trim))
      }
    }.collect()

    output.foreach { case (host, part, outputFile, result) =>
      println(s"$part\t$host\t$outputFile\t$result")
    }
  }  // end of genSort

  def removeOldFiles(numEbsVols: Int = 8): Unit = {
    import scala.sys.process._
    sc.parallelize(1 to 1000, 1000).mapPartitions { iter =>
      for (i <- 0 until numEbsVols) {
        val cmd = s"rm -rf /vol$i/sort*"
        println(cmd)
        cmd.!!
      }
      Iterator.empty
    }.count()
  }
}


class NodeLocalRDDPartition(val index: Int, val node: String) extends Partition


abstract class NodeLocalRDD[T: ClassTag](sc: SparkContext, numParts: Int, hosts: Array[String])
  extends RDD[T](sc, Nil) with Logging {

  override def getPreferredLocations(split: Partition): Seq[String] = {
    Seq(split.asInstanceOf[NodeLocalRDDPartition].node)
  }

  override protected def getPartitions: Array[Partition] = Array.tabulate(numParts) { i =>
    new NodeLocalRDDPartition(i, hosts(i % hosts.length))
  }
}

