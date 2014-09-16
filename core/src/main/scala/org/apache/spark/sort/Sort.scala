package org.apache.spark.sort

import java.io._

import org.apache.spark.rdd.{RDD, ShuffledRDD}
import org.apache.spark.{TaskContext, Partition, SparkContext, SparkConf}
import org.apache.spark.SparkContext._


object Sort {

  def main(args: Array[String]): Unit = {
    val sizeInGB = args(0).toInt
    val numParts = args(1).toInt
    val numEbsVols = 8

    val conf = new SparkConf()
    val bufSize = conf.getInt("spark.sort.buf.size", 4 * 1024 * 1024)

    val sc = new SparkContext(new SparkConf())
    val input = createInputRDD(sc, sizeInGB, numParts, bufSize, numEbsVols)
    val partitioner = new TeraSortPartitioner(numParts)

    val hosts = Sort.readSlaves()

    val sorted = new ShuffledRDD(input, partitioner)
      .setKeyOrdering(new TeraSortOrdering)

    val recordsAfterSort = sorted.mapPartitionsWithIndex { (part, iter) =>
      val volIndex = part % numEbsVols
      val baseFolder = s"/vol$volIndex/sort-${sizeInGB}g-$numParts-out"
      if (!new File(baseFolder).exists()) {
        new File(baseFolder).mkdirs()
      }

      val outputFile = s"$baseFolder/part$part.dat"
      val count = writePartFile(outputFile, iter)

      Iterator(count)
    }.sum()

    println("total number of records: " + recordsAfterSort)
  }

  def createInputRDD(sc: SparkContext, sizeInGB: Int, numParts: Int, bufSize: Int, numEbsVols: Int)
  : RDD[(Array[Byte], Array[Byte])] = {

    val sizeInBytes = sizeInGB.toLong * 1000 * 1000 * 1000
    val numRecords = sizeInBytes / 100
    val recordsPerPartition = math.ceil(numRecords.toDouble / numParts).toLong

    val hosts = Sort.readSlaves()
    new NodeLocalRDD[(Array[Byte], Array[Byte])](sc, numParts, hosts) {
      override def compute(split: Partition, context: TaskContext) = {
        val part = split.index
        val host = split.asInstanceOf[NodeLocalRDDPartition].node

        val start = recordsPerPartition * part
        val volIndex = part % numEbsVols

        val baseFolder = s"/vol$volIndex/sort-${sizeInGB}g-$numParts"
        val outputFile = s"$baseFolder/part$part.dat"

        readPartFile(outputFile, bufSize)
      }
    }
  }

  def readPartFile(file: String, bufSize: Int): Iterator[(Array[Byte], Array[Byte])] = {
    val fileSize = new File(file).length
    assert(fileSize % 100 == 0)
    val numRecords = fileSize / 100

    val is = new BufferedInputStream(new FileInputStream(file), bufSize)
    new Iterator[(Array[Byte], Array[Byte])] {
      private[this] var pos = 0
      override def hasNext: Boolean = pos < numRecords
      override def next(): (Array[Byte], Array[Byte]) = {
        pos += 1
        val key = new Array[Byte](10)
        val value = new Array[Byte](90)
        is.read(key)
        is.read(value)
        (key, value)
      }
    }
  }

  def writePartFile(file: String, iter: Iterator[(Array[Byte], Array[Byte])]): Long = {
    val os = new BufferedOutputStream(new FileOutputStream(file), 4 * 1024 * 1024)
    var record: (Array[Byte], Array[Byte]) = null
    var count: Long = 0
    while (iter.hasNext) {
      record = iter.next()
      os.write(record._1)
      os.write(record._2)
      count += 1
    }
    os.close()
    count
  }

  def readSlaves(): Array[String] = {
    scala.io.Source.fromFile("/root/hosts.txt").getLines().toArray
  }

  /** Run a command, and return exit code, stdout, and stderr. */
  def runCommand(cmd: String): (Int, String, String) = {
    println("running system command: " + cmd)
    val pb = new java.lang.ProcessBuilder(cmd.split(" ") : _*)
    val p = pb.start()
    val exitCode = p.waitFor()

    def read(is: InputStream): String = {
      val buf = new StringBuffer
      val b = new BufferedReader(new InputStreamReader(is))
      var line = b.readLine()
      while (line != null) {
        buf.append(line + "\n")
        line = b.readLine()
      }
      b.close()
      buf.toString
    }

    val stdout = read(p.getInputStream)
    val stderr = read(p.getErrorStream)
    println(s"=====================\nstdout for $cmd:\n$stdout\n==========================")
    println(s"=====================\nstderr for $cmd:\n$stderr\n==========================")
    (exitCode, stdout, stderr)
  }
}
