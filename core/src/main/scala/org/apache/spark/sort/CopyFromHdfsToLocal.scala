package org.apache.spark.sort

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{LocatedFileStatus, RemoteIterator, Path}
import org.apache.spark.{TaskContext, Partition, SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer


object CopyFromHdfsToLocal {

  def main(args: Array[String]) {
    val hdfsFolder = args(0)
    val localFolder = args(1)

    val sc = new SparkContext(new SparkConf().setAppName(
      s"CopyFromHdfsToLocal $hdfsFolder $localFolder"))

    val conf = new Configuration()
    val fs = org.apache.hadoop.fs.FileSystem.get(conf)

    val statuses: RemoteIterator[LocatedFileStatus] = fs.listLocatedStatus(new Path(hdfsFolder))

    // ArrayBuffer of (file path, preferred location)
    val files = new ArrayBuffer[(String, String)]
    var i = 0
    while (statuses.hasNext) {
      val status = statuses.next()
      val blocks = status.getBlockLocations

      val file = status.getPath.toUri.toString
      val loc = blocks.head.getHosts.head
      files += ((file, loc))
      i += 1
    }

    val inputs = files.toArray

    val out = new NodeLocalRDD[(Int, String, String)](sc, files.length, inputs.map(_._2)) {
      override def compute(split: Partition, context: TaskContext) = {
        val input = inputs(split.index)._1
        Iterator(Utils.runCommand(
          s"/root/ephemeral-hdfs/bin/hdfs dfs -copyToLocal $input $localFolder/"))
      }
    }.collect()

    out.foreach(println)
  }
}
