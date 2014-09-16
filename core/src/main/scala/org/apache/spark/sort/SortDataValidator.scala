package org.apache.spark.sort

import java.io.{FileOutputStream, RandomAccessFile, File}

import org.apache.spark.{TaskContext, Partition, SparkConf, SparkContext}


object SortDataValidator {

  def main(args: Array[String]): Unit = {
    val sizeInGB = args(0).toInt
    val numParts = args(1).toInt
    val sc = new SparkContext(new SparkConf())
    genSort(sc, sizeInGB, numParts)
  }

  def genSort(sc: SparkContext, sizeInGB: Int, numParts: Int, numEbsVols: Int = 8): Unit = {

    val sizeInBytes = sizeInGB.toLong * 1000 * 1000 * 1000
    val numRecords = sizeInBytes / 100
    val recordsPerPartition = math.ceil(numRecords.toDouble / numParts).toLong

    val hosts = Sort.readSlaves()

    val output = new NodeLocalRDD[(String, Int, String, String, String, Array[Byte])](sc, numParts, hosts) {
      override def compute(split: Partition, context: TaskContext) = {
        val part = split.index
        val host = split.asInstanceOf[NodeLocalRDDPartition].node

        val start = recordsPerPartition * part
        val volIndex = part % numEbsVols

        val baseFolder = s"/vol$volIndex/sort-${sizeInGB}g-$numParts"
        if (!new File(baseFolder).exists()) {
          new File(baseFolder).mkdirs()
        }

        val outputFile = s"$baseFolder/part$part.dat"
        val checksumOutput = s"$baseFolder/part$part.sum"
        val cmd = s"/root/gensort/64/valsort -o $checksumOutput $outputFile"
        val (exitCode, stdout, stderr) = Sort.runCommand(cmd)

        val checksumData: Array[Byte] = {
          val len = new File(checksumOutput).length()
          val buf = new Array[Byte](len.toInt)
          new RandomAccessFile(checksumOutput, "r").read(buf)
          buf
        }

        Iterator((host, part, outputFile, stdout, stderr, checksumData))
      }
    }.collect()

    output.foreach { case (host, part, outputFile, stdout, stderr, _) =>
      println(s"$part\t$host\t$outputFile\t$stdout\t$stderr")
    }

    val checksumFile = s"/root/sort-${sizeInGB}g-$numParts-out.sum"
    println(s"checksum output: $checksumFile")
    val writer = new FileOutputStream(new File(checksumFile))
    output.foreach {  case (_, _, _, _, _, checksumData: Array[Byte]) =>
      writer.write(checksumData)
    }
    writer.close()
  }  // end of genSort
}
