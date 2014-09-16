package org.apache.spark.sort

import java.io.{FilenameFilter, FileOutputStream, RandomAccessFile, File}

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

    val output = sc.parallelize(1 to hosts.length, hosts.length).mapPartitions { iter =>

      (0 until numEBS).iterator.flatMap { ebs =>
        val baseFolder = new File(s"/vol$ebs/sort-${sizeInGB}g-$numParts-out")
        val files = baseFolder.listFiles(new FilenameFilter {
          override def accept(dir: File, filename: String): Boolean = {
            filename.endsWith(".dat")
          }
        })

        files.iterator.map { file: File =>
          val outputFile = file.getAbsolutePath
          val checksumOutput = outputFile.replace(".dat", ".sum")
          val cmd = s"/root/gensort/64/valsort -o $checksumOutput $outputFile"

          val (exitCode, stdout, stderr) = Sort.runCommand(cmd)

          val checksumData: Array[Byte] = {
            val len = new File(checksumOutput).length()
            val buf = new Array[Byte](len.toInt)
            new RandomAccessFile(checksumOutput, "r").read(buf)
            buf
          }

          val partIndex = "(\\d+)\\.dat".r.findFirstIn(outputFile).get.toInt

          (partIndex, stdout, stderr, checksumData)
        }
      }
    }.collect()

    output.foreach { case (part, stdout, stderr, _) =>
      println(s"$part\t$stdout\t$stderr")
    }

    val checksumFile = s"/root/sort-${sizeInGB}g-$numParts-out.sum"
    println(s"checksum output: $checksumFile")
    val writer = new FileOutputStream(new File(checksumFile))
    output.foreach {  case (_, _, _, checksumData: Array[Byte]) =>
      writer.write(checksumData)
    }
    writer.close()
  }  // end of genSort
}
