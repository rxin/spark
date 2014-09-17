package org.apache.spark.sort

import java.io._

import scala.sys.process._

import org.apache.spark.{TaskContext, Partition, SparkConf, SparkContext}


object XOR {

  def main(args: Array[String]): Unit = {
    val folderName = args(0)  // sort-10g-100 or sort-10g-100-out
    val sc = new SparkContext(new SparkConf())
    validate(sc, folderName)
  }

  def validate(sc: SparkContext, folderName: String): Unit = {

    val hosts = Sort.readSlaves()
    val numEBS = UnsafeSort.NUM_EBS

    // First find all the files
    val output: Array[(Int, String, String)] = new NodeLocalRDD[(Int, String, String)](sc, hosts.length, hosts) {
      override def compute(split: Partition, context: TaskContext) = {
        (0 until numEBS).iterator.flatMap { ebs =>
          val host = "hostname".!!.trim

          val baseFolder = new File(s"/vol$ebs/$folderName")
          val files: Array[File] = baseFolder.listFiles(new FilenameFilter {
            override def accept(dir: File, filename: String): Boolean = {
              filename.endsWith(".dat")
            }
          })

          if (files != null) {
            files.iterator.map { file: File =>
              val outputFile = file.getAbsolutePath
              val partIndex = "(\\d+)\\.dat".r.findFirstIn(outputFile).get.replace(".dat", "").toInt
              println((partIndex, host, outputFile))
              (partIndex, host, outputFile)
            }
          } else {
            Seq.empty[(Int, String, String)]
          }
        }
      }
    }.collect()

    output.sorted.foreach(println)

    val distinctParts = output.map(_._1).distinct.size
    if (output.size != distinctParts) {
      println(s"output size ${output.size} != distinct size $distinctParts")
      System.exit(1)
    }

    val outputMap: Map[Int, String] = output.map(t => (t._1, t._3)).toMap
    val checksumOut = new NodeLocalRDD[(Long, Array[Byte])](
      sc, output.size, output.sorted.map(_._2).toArray) {
      override def compute(split: Partition, context: TaskContext) = {
        val file = outputMap(split.index)

        val fileSize = new File(file).length
        assert(fileSize % 100 == 0)
        var pos = 0
        val is = new BufferedInputStream(new FileInputStream(file), 4 * 1024 * 1024)
        val buf = new Array[Byte](100)
        val checksum = new Array[Byte](100)
        while (pos < fileSize) {
          assert(is.read(buf) == 100)
          xor(checksum, buf)
          pos += 100
        }
        is.close()
        Iterator((fileSize / 100, checksum))
      }
    }.collect()

    val checksum = new Array[Byte](100)
    var numRecords = 0L
    checksumOut.foreach { case (count, input) =>
      xor(checksum, input)
      numRecords += count
    }

    println("num records: " + numRecords)
    println("xor checksum: " + checksum.toSeq)
  }  // end of genSort

  def xor(checksum: Array[Byte], input: Array[Byte]) {
    var i = 0
    while (i < 100) {
      checksum(i) = (checksum(i) ^ input(i)).toByte
      i += 1
    }
  }
}
