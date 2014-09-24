package org.apache.spark.sort

import java.io._
import java.nio.ByteBuffer
import java.util.concurrent.atomic.AtomicInteger

import _root_.io.netty.buffer.ByteBuf
import org.apache.spark._
import org.apache.spark.sort.SortUtils._
import org.apache.spark.network.{ManagedBuffer, FileSegmentManagedBuffer, NettyManagedBuffer}
import org.apache.spark.rdd.{ShuffledRDD, RDD}
import org.apache.spark.util.collection.{Sorter, SortDataFormat}


/**
 * Generate data on the fly, sort, and don't write the output out.
 *
 * This is for testing shuffling and sorting without input/output.
 */
object GenerateDataAndSort extends Logging {

  val NUM_EBS = 8

  private[this] val numTasksOnExecutor = new AtomicInteger

  def main(args: Array[String]): Unit = {
    val sizeInGB = args(0).toInt
    val numParts = args(1).toInt

    val sizeInBytes = sizeInGB.toLong * 1000 * 1000 * 1000
    val numRecords = sizeInBytes / 100
    val recordsPerPartition = math.ceil(numRecords.toDouble / numParts).toLong

    val sc = new SparkContext(
      new SparkConf().setAppName(s"GenerateDataAndSort - $sizeInGB GB - $numParts partitions"))
    val input = createInputRDDUnsafe(sc, sizeInGB, numParts)

    val partitioner = new UnsafePartitioner(numParts)
    val shuffled = new ShuffledRDD(input, partitioner)
      .setSerializer(new UnsafeSerializer(recordsPerPartition))

    val recordsAfterSort: Long = shuffled.mapPartitionsWithIndex { (part, iter) =>

      // Pick the EBS volume to write to. Rotate through the EBS volumes to balance.
      val volIndex = numTasksOnExecutor.getAndIncrement() % NUM_EBS
      val baseFolder = s"/vol$volIndex/sort-${sizeInGB}g-$numParts-out"
      val outputFile = s"$baseFolder/part$part.dat"

      val startTime = System.currentTimeMillis()
      val sortBuffer = sortBuffers.get()
      assert(sortBuffer != null)
      var offset = 0L
      var numShuffleBlocks = 0

      while (iter.hasNext) {
        val n = iter.next()
        val a = n._2.asInstanceOf[ManagedBuffer]
        assert(a.size % 100 == 0, s"shuffle block size ${a.size} is wrong")

        a match {
          case buf: NettyManagedBuffer =>
            val bytebuf = buf.convertToNetty().asInstanceOf[ByteBuf]
            val len = bytebuf.readableBytes()
            assert(len % 100 == 0)
            assert(bytebuf.hasMemoryAddress)

            val start = bytebuf.memoryAddress + bytebuf.readerIndex
            UNSAFE.copyMemory(start, sortBuffer.address + offset, len)
            offset += len
            bytebuf.release()

          case buf: FileSegmentManagedBuffer =>
            val fs = new FileInputStream(buf.file)
            val channel = fs.getChannel
            channel.position(buf.offset)
            assert(buf.length < sortBuffer.ioBuf.capacity)
            sortBuffer.ioBuf.clear()
            sortBuffer.ioBuf.limit(buf.length.toInt)
            sortBuffer.setIoBufAddress(sortBuffer.address + offset)
            val read0 = channel.read(sortBuffer.ioBuf)
            assert(read0 == buf.length)
            offset += read0
            channel.close()
            fs.close()
        }

        numShuffleBlocks += 1
      }

      val timeTaken = System.currentTimeMillis() - startTime
      logInfo(s"XXX Reduce: $timeTaken ms to fetch $numShuffleBlocks shuffle blocks ($offset bytes) $outputFile")
      println(s"XXX Reduce: $timeTaken ms to fetch $numShuffleBlocks shuffle blocks ($offset bytes) $outputFile")

      buildLongPointers(sortBuffer, offset)

      val numRecords = (offset / 100).toInt

      // Sort!!!
      {
        val startTime = System.currentTimeMillis
        sortWithKeys(sortBuffer, numRecords)
        val timeTaken = System.currentTimeMillis - startTime
        logInfo(s"XXX Reduce: Sorting $numRecords records took $timeTaken ms $outputFile")
        println(s"XXX Reduce: Sorting $numRecords records took $timeTaken ms $outputFile")
        scala.Console.flush()
      }

      Iterator(numRecords.toLong)
    }.reduce(_ + _)

    println("total number of records: " + recordsAfterSort)
  }

  def buildLongPointers(sortBuffer: SortBuffer, bufferSize: Long) {
    val startTime = System.currentTimeMillis()
    // Create the pointers array
    var pos = 0L
    var i = 0
    val pointers = sortBuffer.pointers
    while (pos < bufferSize) {
      pointers(i) = sortBuffer.address + pos
      pos += 100
      i += 1
    }
    val timeTaken = System.currentTimeMillis() - startTime
    logInfo(s"XXX finished building index, took $timeTaken ms")
    println(s"XXX finished building index, took $timeTaken ms")
    scala.Console.flush()
  }

  def createInputRDDUnsafe(sc: SparkContext, sizeInGB: Int, numParts: Int)
  : RDD[(Long, Array[Long])] = {

    val sizeInBytes = sizeInGB.toLong * 1000 * 1000 * 1000
    val totalRecords = sizeInBytes / 100
    val recordsPerPartition = math.ceil(totalRecords.toDouble / numParts).toLong

    sc.parallelize(1 to numParts, numParts).mapPartitionsWithIndex { (part, iter) =>
      val iter = datagen.SortDataGenerator.generatePartition(part, recordsPerPartition.toInt)

      if (sortBuffers.get == null) {
        // Allocate 10% overhead since after shuffle the partitions can get slightly uneven.
        val capacity = recordsPerPartition + recordsPerPartition / 10
        sortBuffers.set(new SortBuffer(capacity))
      }

      val sortBuffer = sortBuffers.get()
      var addr: Long = sortBuffer.address

      {
        val startTime = System.currentTimeMillis
        while (iter.hasNext) {
          val buf = iter.next()
          UNSAFE.copyMemory(buf, BYTE_ARRAY_BASE_OFFSET, null, addr, 100)
          addr += 100
        }
        val timeTaken = System.currentTimeMillis - startTime
        logInfo(s"XXX creating $recordsPerPartition records took $timeTaken ms")
      }
      assert(addr - sortBuffer.address == 100L * recordsPerPartition)

      buildLongPointers(sortBuffer, addr - sortBuffer.address)

      // Sort!!!
      {
        val startTime = System.currentTimeMillis
        //val sorter = new Sorter(new LongArraySorter).sort(
        //  sortBuffer.pointers, 0, recordsPerPartition.toInt, ord)
        sortWithKeys(sortBuffer, recordsPerPartition.toInt)
        val timeTaken = System.currentTimeMillis - startTime
        logInfo(s"XXX Sorting $recordsPerPartition records took $timeTaken ms")
        println(s"XXX Sorting $recordsPerPartition records took $timeTaken ms")
        scala.Console.flush()
      }

      Iterator((recordsPerPartition, sortBuffer.pointers))
    }
  }
}
