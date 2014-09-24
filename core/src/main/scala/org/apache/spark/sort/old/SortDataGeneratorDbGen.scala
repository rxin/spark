package org.apache.spark.sort.old

import java.io.File
import java.util.concurrent.atomic.AtomicInteger

import org.apache.spark.sort.{NodeLocalRDD, NodeLocalRDDPartition}
import org.apache.spark.{Partition, SparkConf, SparkContext, TaskContext}


object SortDataGeneratorDbGen {

  private[this] val numTasksOnExecutor = new AtomicInteger

  def main(args: Array[String]): Unit = {
    val sizeInGB = args(0).toInt
    val numParts = args(1).toInt
    val dirs = args(2).split(",").map(_ + s"/sort-${sizeInGB}g-$numParts").toSeq
    val sc = new SparkContext(
      new SparkConf().setAppName(s"SortDataGeneratorDbGen - $sizeInGB GB - $numParts partitions"))
    genSort(sc, sizeInGB, numParts, dirs)
  }

  def genSort(sc: SparkContext, sizeInGB: Int, numParts: Int, dirs: Seq[String]): Unit = {

    val sizeInBytes = sizeInGB.toLong * 1000 * 1000 * 1000
    val numRecords = sizeInBytes / 100
    val recordsPerPartition = math.ceil(numRecords.toDouble / numParts).toLong

    val hosts = Sort.readSlaves()

    val output = new NodeLocalRDD[(String, Int, String, String, String)](sc, numParts, hosts) {
      override def compute(split: Partition, context: TaskContext) = {
        val part = split.index
        val host = split.asInstanceOf[NodeLocalRDDPartition].node

        val start = recordsPerPartition * part
        val baseFolder = dirs(numTasksOnExecutor.getAndIncrement() % dirs.length)
        if (!new File(baseFolder).exists()) {
          new File(baseFolder).mkdirs()
        }

        val outputFile = s"$baseFolder/part$part.dat"
        val cmd = s"/root/gensort/64/gensort -c -b$start -t1 $recordsPerPartition $outputFile"
        val (exitCode, stdout, stderr) = Sort.runCommand(cmd)
        Iterator((host, part, outputFile, stdout, stderr))
      }
    }.collect()

    output.foreach { case (host, part, outputFile, stdout, stderr) =>
      println(s"$part\t$host\t$outputFile\t$stdout\t$stderr")
    }

    val checksumFile = s"/root/sort-${sizeInGB}g-$numParts.sum"
    println(s"checksum output: $checksumFile")
    val writer = new java.io.PrintWriter(new File(checksumFile))
    output.foreach {  case (host, part, outputFile, stdout, stderr: String) =>
      writer.write(stderr)
    }
    writer.close()
  }  // end of genSort
}
