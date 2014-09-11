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

import java.io._
import scala.sys.process._

import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag


object Sort {
  def main(args: Array[String]): Unit = {
    val numParts = args(0).toInt

  }

  def readSlaves(): Array[String] = {
    scala.io.Source.fromFile("/root/hosts.txt").getLines().toArray
  }

  /** Run a command, and return exit code, stdout, and stderr. */
  def runCommand(cmd: String): (Int, String, String) = {
    println("running system command: " + cmd)
    val pb = new java.lang.ProcessBuilder(cmd.split(" ") : _*)
    val p = pb.start()
    val exitCode = p.waitFor()

    def read(is: InputStream): String = {
      val buf = new StringBuffer
      val b = new BufferedReader(new InputStreamReader(is))
      var line = b.readLine()
      while (line != null) {
        buf.append(line)
        line = b.readLine()
      }
      b.close()
      buf.toString
    }

    val stdout = read(p.getInputStream)
    val stderr = read(p.getErrorStream)
    println(s"=====================\nstdout for $cmd:\n$stdout\n==========================")
    println(s"=====================\nstderr for $cmd:\n$stderr\n==========================")
    (exitCode, stdout, stderr)
  }
}


object SortGenerateHosts {
  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(new SparkConf())
    val hosts = sc.parallelize(1 to 1000, 1000).mapPartitions { iter =>
      val host = "hostname".!!
      Iterator(host.trim)
    }.collect().distinct.sorted.toArray

    val writer = new java.io.PrintWriter(new File("/root/hosts.txt"))
    hosts.foreach { host => writer.write(host + "\n") }
    writer.close()
    println(s"written ${hosts.size} hosts to /root/hosts.txt")
  }
}



object SortDataValidator {

}



object SortDataGenerator {

  def main(args: Array[String]): Unit = {
    val sizeInGB = args(0).toInt
    val numParts = args(1).toInt
    val sc = new SparkContext(new SparkConf())
    genSort(sc, sizeInGB, numParts)
  }

  def genSort(sc: SparkContext, sizeInGB: Int, numParts: Int, numEbsVols: Int = 8): Unit = {

    val sizeInBytes = sizeInGB * 1000 * 1000 * 1000
    val numRecords = (sizeInBytes / 100).toLong
    val recordsPerPartition = math.ceil(numRecords.toDouble / numParts).toLong

    val hosts = Sort.readSlaves()

    val output = new NodeLocalRDD[(String, Int, String, String, String)](sc, numParts, hosts) {
      override def compute(split: Partition, context: TaskContext) = {
        val part = split.index
        val host = split.asInstanceOf[NodeLocalRDDPartition].node

        val start = recordsPerPartition * part
        val volIndex = part % numEbsVols

        val baseFolder = s"/vol$volIndex/sort-${sizeInGB}g"
        if (!new File(baseFolder).exists()) {
          new File(baseFolder).mkdirs()
        }

        val outputFile = s"$baseFolder/part$part"
        val cmd = s"/root/gensort/64/gensort -c -b$start -t1 $recordsPerPartition $outputFile"
        val (exitCode, stdout, stderr) = Sort.runCommand(cmd)
        Iterator((host, part, outputFile, stdout, stderr))
      }
    }.collect()

    output.foreach { case (host, part, outputFile, stdout, stderr) =>
      println(s"$part\t$host\t$outputFile\t$stdout\t$stderr")
    }
  }  // end of genSort
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
