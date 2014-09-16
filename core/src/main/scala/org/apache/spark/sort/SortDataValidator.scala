package org.apache.spark.sort

import java.io.{FilenameFilter, FileOutputStream, RandomAccessFile, File}

import scala.sys.process._

import org.apache.spark.{TaskContext, Partition, SparkConf, SparkContext}


object SortDataValidator {

  def main(args: Array[String]): Unit = {
    val sizeInGB = args(0).toInt
    val numParts = args(1).toInt
    val sc = new SparkContext(new SparkConf())
    validate(sc, sizeInGB, numParts)
  }

  def validate(sc: SparkContext, sizeInGB: Int, numParts: Int): Unit = {

    val hosts = Sort.readSlaves()
    val numEBS = UnsafeSort.NUM_EBS

    val output: Array[(Int, String, String)] =
      sc.parallelize(1 to hosts.length, hosts.length).mapPartitions { iter =>
        (0 until numEBS).iterator.flatMap { ebs =>
          val host = "hostname".!!.trim

          val baseFolder = new File(s"/vol$ebs/sort-${sizeInGB}g-$numParts-out")
          val files: Array[File] = baseFolder.listFiles(new FilenameFilter {
            override def accept(dir: File, filename: String): Boolean = {
              filename.endsWith(".dat")
            }
          })

          if (files != null) {
            files.iterator.map { file: File =>
              val outputFile = file.getAbsolutePath
              val partIndex = "(\\d+)\\.dat".r.findFirstIn(outputFile).get.replace(".dat", "").toInt
              (partIndex, host, outputFile)
            }
          } else {
            Seq.empty[(Int, String, String)]
          }
        }
      }.collect()

    output.sortBy(_._1).foreach(println)

    val distinctParts = output.map(_._1).distinct.size
    if (output.size != distinctParts) {
      println(s"output size ${output.size} != distinct size $distinctParts")
    }

    val outputMap: Map[Int, String] = output.map(t => (t._1, t._3)).toMap
    val checksumOut = new NodeLocalRDD[(Int, String, String, String, Array[Byte])](
      sc, output.size, output.map(_._2).toArray) {
      override def compute(split: Partition, context: TaskContext) = {
        val outputFile = outputMap(split.index)
        val checksumOutput = outputFile.replace(".dat", ".sum")
        val cmd = s"/root/gensort/64/valsort -o $checksumOutput $outputFile"
        val (exitCode, stdout, stderr) = Sort.runCommand(cmd)

        val checksumData: Array[Byte] = {
          val len = new File(checksumOutput).length()
          val buf = new Array[Byte](len.toInt)
          new RandomAccessFile(checksumOutput, "r").read(buf)
          buf
        }

        Iterator((split.index, outputFile, stdout, stderr, checksumData))
      }
    }.collect()

    checksumOut.foreach { case (part, outputFile, stdout, stderr, _) =>
      println(s"$part\t$outputFile\t$stdout\t$stderr")
    }

    val checksumFile = s"/root/sort-${sizeInGB}g-$numParts-out.sum"
    println(s"checksum output: $checksumFile")
    val writer = new FileOutputStream(new File(checksumFile))
    checksumOut.foreach {  case (_, _, _, _, checksumData: Array[Byte]) =>
      writer.write(checksumData)
    }
    writer.close()
  }  // end of genSort
}
