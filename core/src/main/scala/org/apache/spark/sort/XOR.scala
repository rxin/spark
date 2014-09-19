package org.apache.spark.sort

import java.io._
import java.util.zip.CRC32

import com.google.common.primitives.UnsignedBytes

import scala.sys.process._

import org.apache.spark.{TaskContext, Partition, SparkConf, SparkContext}


object XOR {

  def main(args: Array[String]): Unit = {
    val folderName = args(0)  // sort-10g-100 or sort-10g-100-out
    val sc = new SparkContext(
        new SparkConf().setAppName(s"XOR - $folderName"))
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
    val checksumOut = new NodeLocalRDD[(Int, Long, Array[Byte], Array[Byte], Array[Byte], Long)](
      sc, output.size, output.sorted.map(_._2).toArray) {
      override def compute(split: Partition, context: TaskContext) = {
        val part = split.index
        val file = outputMap(split.index)

        val cmp = UnsignedBytes.lexicographicalComparator()

        val fileSize = new File(file).length
        assert(fileSize % 100 == 0)
        var pos = 0
        val min = new Array[Byte](10)
        val max = new Array[Byte](10)
        val is = new BufferedInputStream(new FileInputStream(file), 4 * 1024 * 1024)
        val lastRecord = new Array[Byte](100)
        val buf = new Array[Byte](100)
        val checksum = new Array[Byte](100)
        val crc32 = new CRC32
        while (pos < fileSize) {
          assert(is.read(buf) == 100)
          xor(checksum, buf)
          pos += 100

          // Make sure current record >= previous record
          assert(cmp.compare(buf, lastRecord) >= 0)

          // Compute crc32
          crc32.update(buf)

          // Set partition min and max
          if (pos == 100) {
            System.arraycopy(buf, 0, min, 0, 10)
          } else if (pos == fileSize) {
            System.arraycopy(buf, 0, max, 0, 10)
          }
        }
        is.close()
        Iterator((part, fileSize / 100, checksum, min, max, crc32.getValue))
      }
    }.collect()

    val cmp = UnsignedBytes.lexicographicalComparator()

    val checksum = new Array[Byte](100)
    var numRecords = 0L
    var lastMax = new Array[Byte](10)
    checksumOut.foreach { case (part, count, input, min, max, crc32) =>
      xor(checksum, input)
      numRecords += count
    }
    println("num records: " + numRecords)
    println("xor checksum: " + checksum.toSeq)

    checksumOut.foreach { case (part, count, input, min, max, crc32) =>
      println(s"part $part")
      println(s"min " + min.toSeq)
      println(s"max " + max.toSeq)
      println(s"crc32 " + crc32 + " " + java.lang.Long.toHexString(crc32))

      assert(cmp.compare(min, max) < 0, "min >= max")
      assert(cmp.compare(lastMax, min) < 0, "current partition min < last partition max")
      lastMax = max
    }

    println("partitions are properly sorted")
  }  // end of genSort

  def xor(checksum: Array[Byte], input: Array[Byte]) {
    var i = 0
    while (i < 100) {
      checksum(i) = (checksum(i) ^ input(i)).toByte
      i += 1
    }
  }
}
