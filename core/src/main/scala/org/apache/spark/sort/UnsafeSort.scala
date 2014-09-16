package org.apache.spark.sort

import java.io._

import org.apache.spark.util.MutablePair
import org.apache.spark.{SparkConf, TaskContext, Partition, SparkContext}
import org.apache.spark.rdd.{ShuffledRDD, RDD}
import org.apache.spark.SparkContext._


class MutableLong(var value: Long)


/**
 * A version of the sort code that uses Unsafe to allocate off-heap blocks.
 *
 * See also [[UnsafeSerializer]] and [[UnsafeOrdering]].
 */
object UnsafeSort {

  val NUM_EBS = 8

  def main(args: Array[String]): Unit = {
    val sizeInGB = args(0).toInt
    val numParts = args(1).toInt

    val conf = new SparkConf()
    val bufSize = conf.getInt("spark.sort.buf.size", 4 * 1024 * 1024)

    val sizeInBytes = sizeInGB.toLong * 1000 * 1000 * 1000
    val numRecords = sizeInBytes / 100
    val recordsPerPartition = math.ceil(numRecords.toDouble / numParts).toLong

    val sc = new SparkContext(new SparkConf())
    val input = createInputRDDUnsafe(sc, sizeInGB, numParts, bufSize)

    val hosts = Sort.readSlaves()

    val sorted = new ShuffledRDD(input, new UnsafePartitioner(numParts))
      .setKeyOrdering(new UnsafeOrdering)
      .setSerializer(new UnsafeSerializer(recordsPerPartition))

    val recordsAfterSort = sorted.mapPartitionsWithIndex { (part, iter) =>
      val volIndex = part % NUM_EBS
      val baseFolder = s"/vol$volIndex/sort-${sizeInGB}g-$numParts-out"
      if (!new File(baseFolder).exists()) {
        new File(baseFolder).mkdirs()
      }

      val outputFile = s"$baseFolder/part$part.dat"

      val os = new BufferedOutputStream(new FileOutputStream(outputFile), bufSize)
      val buf = new Array[Byte](100)
      var count = 0
      val arrOffset = BYTE_ARRAY_BASE_OFFSET
      while (iter.hasNext) {
        val addr = iter.next()._1
        UNSAFE.copyMemory(null, addr, buf, arrOffset, 100)
        os.write(buf)
        count += 1
      }
      os.close()
      Iterator(count)
    }.sum()

    println("total number of records: " + recordsAfterSort)
  }

  final val UNSAFE: sun.misc.Unsafe = {
    val unsafeField = classOf[sun.misc.Unsafe].getDeclaredField("theUnsafe")
    unsafeField.setAccessible(true)
    unsafeField.get().asInstanceOf[sun.misc.Unsafe]
  }

  final val BYTE_ARRAY_BASE_OFFSET: Long = UNSAFE.arrayBaseOffset(classOf[Array[Byte]])

  /** A thread local variable storing a pointer to the buffer allocated off-heap. */
  val blocks = new ThreadLocal[java.lang.Long]

  def createInputRDDUnsafe(sc: SparkContext, sizeInGB: Int, numParts: Int, bufSize: Int)
    : RDD[MutablePair[Long, Long]] = {

    val sizeInBytes = sizeInGB.toLong * 1000 * 1000 * 1000
    val numRecords = sizeInBytes / 100
    val recordsPerPartition = math.ceil(numRecords.toDouble / numParts).toLong

    val hosts = Sort.readSlaves()
    new NodeLocalRDD[MutablePair[Long, Long]](sc, numParts, hosts) {
      override def compute(split: Partition, context: TaskContext) = {
        val part = split.index
        val host = split.asInstanceOf[NodeLocalRDDPartition].node

        val start = recordsPerPartition * part
        val volIndex = part % NUM_EBS

        val baseFolder = s"/vol$volIndex/sort-${sizeInGB}g-$numParts"
        val outputFile = s"$baseFolder/part$part.dat"

        val fileSize = new File(outputFile).length
        assert(fileSize % 100 == 0)

        if (blocks.get == null) {
          val blockSize = recordsPerPartition * 100
          logInfo(s"Allocating $blockSize bytes")
          val blockAddress = UNSAFE.allocateMemory(blockSize + blockSize / 5)
          logInfo(s"Allocating $blockSize bytes ... allocated at $blockAddress")
          blocks.set(blockAddress)
        }

        new Iterator[MutablePair[Long, Long]] {
          private[this] var pos: Long = blocks.get.longValue()
          private[this] val endPos: Long = pos + fileSize
          private[this] val arrOffset = BYTE_ARRAY_BASE_OFFSET
          private[this] val buf = new Array[Byte](100)
          private[this] val is = new BufferedInputStream(new FileInputStream(outputFile), bufSize)
          private[this] val tuple = new MutablePair[Long, Long]
          override def hasNext: Boolean = {
            val more = pos < endPos
            if (!more) {
              is.close()
            }
            more
          }
          override def next() = {
            is.read(buf)
            UNSAFE.copyMemory(buf, arrOffset, null, pos, 100)
            tuple._1 = pos
            pos += 100
            tuple
          }
        }
      }
    }
  }
}
