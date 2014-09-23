package org.apache.spark.sort.old

import java.io.{File, FileOutputStream, FilenameFilter, RandomAccessFile}

import org.apache.spark.sort.{NodeLocalRDD, UnsafeSort}
import org.apache.spark.{Partition, SparkConf, SparkContext, TaskContext}

import scala.sys.process._


object SortDataValidator {

  def main(args: Array[String]): Unit = {
    val folderName = args(0)  // sort-10g-100 or sort-10g-100-out
    val dirs = args(1).split(",").map(_ + "/" + folderName).toSeq
    val sc = new SparkContext(new SparkConf().setAppName(s"valgen - " + dirs.mkString(",")))
    validate(sc, dirs, folderName)
  }

  def validate(sc: SparkContext, dirs: Seq[String], folderName: String): Unit = {
    val hosts = Sort.readSlaves()
    // First find all the files
    val output: Array[(Int, String, String)] = new NodeLocalRDD[(Int, String, String)](sc, hosts.length, hosts) {
      override def compute(split: Partition, context: TaskContext) = {
        dirs.iterator.flatMap { dir =>
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
    val checksumOut = new NodeLocalRDD[(Int, String, String, String, Array[Byte])](
      sc, output.size, output.sorted.map(_._2).toArray) {
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

    val checksumFile = s"/root/$folderName.sum"
    println(s"checksum output: $checksumFile")
    if (!new File(s"/root/$folderName-checksums/").exists()) {
      new File(s"/root/$folderName-checksums/").mkdir()
    }

    val writer = new FileOutputStream(new File(checksumFile))
    checksumOut.foreach {  case (part, _, _, _, checksumData: Array[Byte]) =>
      writer.write(checksumData)

      val partFile = s"/root/$folderName-checksums/part$part.sum"
      val fp = new RandomAccessFile(partFile, "rw")
      fp.write(checksumData)
      fp.close()
    }
    writer.close()
  }  // end of genSort
}
