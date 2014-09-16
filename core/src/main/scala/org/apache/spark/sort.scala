/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark

import java.io._

import scala.reflect.ClassTag
import scala.sys.process._

import com.google.common.primitives.Longs
import org.apache.spark.rdd.{ShuffledRDD, RDD}


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

    sorted.mapPartitionsWithIndex { (part, iter) =>
      val volIndex = part % numEbsVols
      val baseFolder = s"/vol$volIndex/sort-${sizeInGB}g-$numParts-out"
      val outputFile = s"$baseFolder/part$part.dat"

      writePartFile(outputFile, iter)
      Iterator(1)
    }.foreach(_ => Unit)

    println("total number of records: " + input.count())
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

  def writePartFile(file: String, iter: Iterator[(Array[Byte], Array[Byte])]): Unit = {
    val os = new BufferedOutputStream(new FileOutputStream(file), 4 * 1024 * 1024)
    var record: (Array[Byte], Array[Byte]) = null
    while (iter.hasNext) {
      record = iter.next()
      os.write(record._1)
      os.write(record._2)
    }
    os.close()
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


object SortGenerateHosts {
  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(new SparkConf())
    val hosts = sc.parallelize(1 to 1000, 1000).mapPartitions { iter =>
      val host = "hostname".!!
      Iterator(host.trim)
    }.collect().distinct.sorted.toArray

    val writer = new java.io.PrintWriter(new File("/root/hosts.txt"))
    hosts.foreach { host => writer.write(host + "\n") }
    writer.close()
    println(s"written ${hosts.size} hosts to /root/hosts.txt")
  }
}


object SortDataGenerator {

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

    val output = new NodeLocalRDD[(String, Int, String, String, String)](sc, numParts, hosts) {
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
        val cmd = s"/root/gensort/64/gensort -c -b$start -t1 $recordsPerPartition $outputFile"
        val (exitCode, stdout, stderr) = Sort.runCommand(cmd)
        Iterator((host, part, outputFile, stdout, stderr))
      }
    }.collect()

    output.foreach { case (host, part, outputFile, stdout, stderr) =>
      println(s"$part\t$host\t$outputFile\t$stdout\t$stderr")
    }

    val checksumFile = s"/root/sort-${sizeInGB}g.sum"
    println(s"checksum output: $checksumFile")
    val writer = new java.io.PrintWriter(new File(checksumFile))
    output.foreach {  case (host, part, outputFile, stdout, stderr: String) =>
      writer.write(stderr)
    }
    writer.close()
  }  // end of genSort
}


class NodeLocalRDDPartition(val index: Int, val node: String) extends Partition


abstract class NodeLocalRDD[T: ClassTag](sc: SparkContext, numParts: Int, hosts: Array[String])
  extends RDD[T](sc, Nil) with Logging {

  override def getPreferredLocations(split: Partition): Seq[String] = {
    Seq(split.asInstanceOf[NodeLocalRDDPartition].node)
  }

  override protected def getPartitions: Array[Partition] = Array.tabulate(numParts) { i =>
    new NodeLocalRDDPartition(i, hosts(i % hosts.length))
  }
}


/**
 * Partitioner for terasort. It uses the first seven bytes of the byte array to partition
 * the key space evenly.
 */
case class TeraSortPartitioner(numPartitions: Int) extends Partitioner {

  import TeraSortPartitioner._

  val rangePerPart = (max - min) / numPartitions

  override def getPartition(key: Any): Int = {
    val b = key.asInstanceOf[Array[Byte]]
    val prefix = Longs.fromBytes(0, b(0), b(1), b(2), b(3), b(4), b(5), b(6))
    (prefix / rangePerPart).toInt
  }
}


object TeraSortPartitioner {
  val min = Longs.fromBytes(0, 0, 0, 0, 0, 0, 0, 0)
  val max = Longs.fromBytes(0, -1, -1, -1, -1, -1, -1, -1)  // 0xff = -1
}


/**
 * Sort ordering for comparing 10-byte arrays.
 *
 * http://grepcode.com/file/repo1.maven.org/maven2/com.google.guava/guava/17.0/com/google/common/primitives/UnsignedBytes.java#298
 */
class TeraSortOrdering extends Ordering[Array[Byte]] {
  import TeraSortOrdering._

  override def compare(left: Array[Byte], right: Array[Byte]): Int = {
    val lw: Long = theUnsafe.getLong(left, BYTE_ARRAY_BASE_OFFSET)
    val rw: Long = theUnsafe.getLong(right, BYTE_ARRAY_BASE_OFFSET)
    if (lw != rw) {
      val n: Int = java.lang.Long.numberOfTrailingZeros(lw ^ rw) & ~0x7
      (((lw >>> n) & UNSIGNED_MASK) - ((rw >>> n) & UNSIGNED_MASK)).asInstanceOf[Int]
    } else {
      // First word not equal. Compare the rest 2 bytes.
      val diff9 = left(8) - right(8)
      if (diff9 != 0) {
        diff9
      } else {
        left(9) - right(9)
      }
    }
  }
}


object TeraSortOrdering {
  private final val MAX_VALUE: Byte = 0xFF.asInstanceOf[Byte]
  private final val UNSIGNED_MASK: Int = 0xFF
  private final val theUnsafe: sun.misc.Unsafe = {
    val unsafeField = classOf[sun.misc.Unsafe].getDeclaredField("theUnsafe")
    unsafeField.setAccessible(true)
    unsafeField.get().asInstanceOf[sun.misc.Unsafe]
  }
  private final val BYTE_ARRAY_BASE_OFFSET: Int = theUnsafe.arrayBaseOffset(classOf[Array[Byte]])
}
