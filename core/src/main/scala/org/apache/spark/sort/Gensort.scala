package org.apache.spark.sort

import java.io.File
import java.util.concurrent.atomic.AtomicInteger

import org.apache.spark.{Partition, SparkConf, SparkContext, TaskContext}


object Gensort {

  private[this] val numTasksOnExecutor = new AtomicInteger

  def main(args: Array[String]): Unit = {
    val sizeInGB = args(0).toInt
    val numParts = args(1).toInt
    val skew = args(2).toBoolean
    val dir = args(3) + s"/sort-${sizeInGB}g-$numParts" + (if (skew) "-skew" else "")
    val sc = new SparkContext(
      new SparkConf().setAppName(s"Gensort - $dir - " + (if (skew) "skewed" else "non-skewed")))
    genSort(sc, sizeInGB, numParts, dir, skew)
  }

  def genSort(sc: SparkContext, sizeInGB: Int, numParts: Int, dir: String, skew: Boolean): Unit = {

    val sizeInBytes = sizeInGB.toLong * 1000 * 1000 * 1000
    val numRecords = sizeInBytes / 100
    val recordsPerPartition = math.ceil(numRecords.toDouble / numParts).toLong

    val hosts = Utils.readSlaves()

    val output = new NodeLocalRDD[(String, Int, String, String, String)](sc, numParts, hosts) {
      override def compute(split: Partition, context: TaskContext) = {
        val part = split.index
        val host = split.asInstanceOf[NodeLocalRDDPartition].node

        val start = recordsPerPartition * part
        if (!new File(dir).exists()) {
          new File(dir).mkdirs()
        }

        val outputFile = s"$dir/part$part.dat"
        val skewFlag = if (skew) "-s" else ""
        val cmd = s"/root/gensort/64/gensort -c $skewFlag -b$start -t1 $recordsPerPartition $outputFile"
        val (exitCode, stdout, stderr) = Utils.runCommand(cmd)
        Iterator((host, part, outputFile, stdout, stderr))
      }
    }.collect()

    output.foreach { case (host, part, outputFile, stdout, stderr) =>
      println(s"$part\t$host\t$outputFile\t$stdout\t$stderr")
    }

    val checksumFile = s"/root/sort-${sizeInGB}g-$numParts-gensort.log"
    println(s"checksum output: $checksumFile")
    val writer = new java.io.PrintWriter(new File(checksumFile))
    output.foreach {  case (host, part, outputFile, stdout, stderr: String) =>
      writer.write(stderr)
    }
    writer.close()
  }  // end of genSort
}
