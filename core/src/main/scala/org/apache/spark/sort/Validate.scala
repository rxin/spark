package org.apache.spark.sort

import java.io._

import scala.sys.process._

import com.google.common.primitives.UnsignedBytes

import org.apache.spark.{TaskContext, Partition, SparkConf, SparkContext}
import org.apache.spark.sort.datagen.{Unsigned16, PureJavaCrc32}


object Validate {

  def main(args: Array[String]): Unit = {
    val dir = args(0)  // /mnt/sort-10g-100
    val sc = new SparkContext(
        new SparkConf().setAppName(s"Validate - $dir"))
    validate(sc, dir)
  }

  def validate(sc: SparkContext, dir: String): Unit = {
    val hosts = Utils.readSlaves()
    // First find all the files
    val output = new NodeLocalRDD[(Int, String, String)](sc, hosts.length, hosts) {
      override def compute(split: Partition, context: TaskContext) = {
        val host = "hostname".!!.trim

        val baseFolder = new File(dir)
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
          Iterator.empty
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
    val checksumOut = new NodeLocalRDD[(Int, Long, Unsigned16, Array[Byte], Array[Byte])](
      sc, output.size, output.sorted.map(_._2).toArray) {
      override def compute(split: Partition, context: TaskContext) = {
        val part = split.index
        val file = outputMap(split.index)

        val cmp = UnsignedBytes.lexicographicalComparator()

        val fileSize = new File(file).length
        assert(fileSize % 100 == 0)
        var pos = 0L
        val min = new Array[Byte](10)
        val max = new Array[Byte](10)
        val is = new BufferedInputStream(new FileInputStream(file), 4 * 1024 * 1024)
        val lastRecord = new Array[Byte](100)
        val buf = new Array[Byte](100)

        val sum = new Unsigned16
        val checksum = new Unsigned16
        val crc32 = new PureJavaCrc32()
        while (pos < fileSize) {
          val read0 = is.read(buf)
          assert(read0 == 100, s"read0 is $read0, pos is $pos, filelen is $fileSize")
          pos += 100

          // Make sure current record >= previous record
          assert(cmp.compare(buf, lastRecord) >= 0)

          crc32.reset()
          crc32.update(buf, 0, buf.length)
          checksum.set(crc32.getValue)
          sum.add(checksum)

          // Set partition min and max
          if (pos == 100) {
            System.arraycopy(buf, 0, min, 0, 10)
          } else if (pos == fileSize) {
            System.arraycopy(buf, 0, max, 0, 10)
          }
        }
        is.close()
        Iterator((part, fileSize / 100, sum, min, max))
      }
    }.collect()

    val cmp = UnsignedBytes.lexicographicalComparator()

    val sum = new Unsigned16
    var numRecords = 0L
    checksumOut.foreach { case (part, count, partSum, min, max) =>
      sum.add(partSum)
      numRecords += count
    }
    println("num records: " + numRecords)
    println("checksum: " + sum.toString)

    var lastMax = new Array[Byte](10)
    checksumOut.foreach { case (part, count, partSum, min, max) =>
      println(s"part $part")
      println(s"min " + min.toSeq.map(x => if (x < 0) 256 + x else x))
      println(s"max " + max.toSeq.map(x => if (x < 0) 256 + x else x))

      assert(cmp.compare(min, max) < 0, "min >= max")
      assert(cmp.compare(lastMax, min) < 0, "current partition min < last partition max")
      lastMax = max
    }

    println("num records: " + numRecords)
    println("checksum: " + sum.toString)

    println("partitions are properly sorted")
  }  // end of genSort
}
