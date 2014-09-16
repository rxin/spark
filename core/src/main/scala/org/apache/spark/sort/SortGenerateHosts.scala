package org.apache.spark.sort

import java.io.File

import scala.sys.process._

import org.apache.spark.{SparkConf, SparkContext}

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
