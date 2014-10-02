package org.apache.spark.sort

import java.io._

import _root_.io.netty.buffer.{ByteBufInputStream, ByteBuf}
import com.google.common.io.ByteStreams
import org.apache.spark._
import org.apache.spark.io.LZFCompressionCodec
import org.apache.spark.sort.SortUtils._
import org.apache.spark.network.{ManagedBuffer, FileSegmentManagedBuffer, NettyManagedBuffer}
import org.apache.spark.rdd.{ShuffledRDD, RDD}


/**
 * Generate data on the fly, sort, and don't write the output out.
 *
 * This is for testing shuffling and sorting without input/output.
 */
object PBSort extends Logging {

  def main(args: Array[String]): Unit = {
    val sizeInGB = args(0).toInt
    val numParts = args(1).toInt

    val sizeInBytes = sizeInGB.toLong * 1000 * 1000 * 1000
    val numRecords = sizeInBytes / 100
    val recordsPerPartition = math.ceil(numRecords.toDouble / numParts).toLong

    val sc = new SparkContext(
      new SparkConf().setAppName(s"PBSort - $sizeInGB GB - $numParts partitions"))
    val input = createInputRDDUnsafe(sc, sizeInGB, numParts)

    val partitioner = new IndyPartitionerPB(numParts)
    val shuffled = new ShuffledRDD(input, partitioner)
      .setSerializer(new UnsafeSerializer(recordsPerPartition))

    val recordsAfterSort: Long = shuffled.mapPartitionsWithIndex { (part, iter) =>
      val sortBuffer = sortBuffers.get()
      assert(sortBuffer != null)
      var numShuffleBlocks = 0

      val codec = new org.apache.spark.io.LZFCompressionCodec(new SparkConf)
      var memoryAddress = sortBuffer.address
      val buf100 = new Array[Byte](100)

      while (iter.hasNext) {
        val n = iter.next()
        val a = n._2.asInstanceOf[ManagedBuffer]

        a match {
          case buf: NettyManagedBuffer =>
            val bytebuf = buf.convertToNetty().asInstanceOf[ByteBuf]

            val is = codec.compressedInputStream(new ByteBufInputStream(bytebuf))
            var stop = false
            while (!stop) {
              val read0 = is.read(buf100)
              if (read0 < 0) {
                stop = true
              } else {
                //assert(read0 == 100, s"read0 is $read0")
                UNSAFE.copyMemory(buf100, BYTE_ARRAY_BASE_OFFSET, null, memoryAddress, read0)
                memoryAddress += read0
              }
            }
            bytebuf.release()

          case buf: FileSegmentManagedBuffer =>
            val fs = new FileInputStream(buf.file)
            fs.skip(buf.offset)

            val is = codec.compressedInputStream(
              new BufferedInputStream(ByteStreams.limit(fs, buf.length), 128 * 1024))
            var stop = false
            while (!stop) {
              val read0 = is.read(buf100)
              if (read0 < 0) {
                stop = true
              } else {
                //assert(read0 == 100, s"read0 is $read0 memory address is $memoryAddress, ${sortBuffer.address}, ${sortBuffer.len}")
                UNSAFE.copyMemory(buf100, BYTE_ARRAY_BASE_OFFSET, null, memoryAddress, read0)
                memoryAddress += read0
                assert(memoryAddress <= sortBuffer.address + sortBuffer.len && memoryAddress > sortBuffer.address,
                  s"memory address is $memoryAddress, ${sortBuffer.address}, ${sortBuffer.len}")
              }
            }
            is.close()
        }

        numShuffleBlocks += 1
      }

      assert((memoryAddress - sortBuffer.address) % 100 == 0,
        s"read ${memoryAddress - sortBuffer.address} bytes")

      buildLongPointers(sortBuffer, memoryAddress - sortBuffer.address)
      val numRecords = ((memoryAddress - sortBuffer.address) / 100).toInt

      // Sort!!!
      sortWithKeys(sortBuffer, numRecords)

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
