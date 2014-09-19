package org.apache.spark.sort

import java.io._
import java.nio.ByteBuffer
import java.nio.channels.FileChannel

import _root_.io.netty.buffer.ByteBuf
import org.apache.spark._
import org.apache.spark.network.{ManagedBuffer, FileSegmentManagedBuffer, NettyManagedBuffer}
import org.apache.spark.rdd.{ShuffledRDD, RDD}
import org.apache.spark.util.collection.{Sorter, SortDataFormat}


/**
 * A version of the sort code that uses Unsafe to allocate off-heap blocks.
 *
 * See also [[UnsafeSerializer]] and [[UnsafeOrdering]].
 */
object UnsafeSort extends Logging {

  val NUM_EBS = 8

  val ord = new UnsafeOrdering

  def main(args: Array[String]): Unit = {
    val sizeInGB = args(0).toInt
    val numParts = args(1).toInt

    val conf = new SparkConf()

    val sizeInBytes = sizeInGB.toLong * 1000 * 1000 * 1000
    val numRecords = sizeInBytes / 100
    val recordsPerPartition = math.ceil(numRecords.toDouble / numParts).toLong

    val sc = new SparkContext(
      new SparkConf().setAppName(s"UnsafeSort - $sizeInGB GB - $numParts partitions"))
    val input = createInputRDDUnsafe(sc, sizeInGB, numParts)

    val hosts = Sort.readSlaves()

    val partitioner = new UnsafePartitioner(numParts)
    val sorted = new ShuffledRDD(input, partitioner)
      .setKeyOrdering(new UnsafeOrdering)
      .setSerializer(new UnsafeSerializer(recordsPerPartition))

    val recordsAfterSort: Long = sorted.mapPartitionsWithIndex { (part, iter) =>

      val startTime = System.currentTimeMillis()
      val sortBuffer = UnsafeSort.sortBuffers.get()
      assert(sortBuffer != null)
      var offset = 0L
      var numShuffleBlocks = 0

      println("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
      scala.Console.flush()

      while (iter.hasNext) {
        val a = iter.next()._2.asInstanceOf[ManagedBuffer]
        a match {
          case buf: NettyManagedBuffer =>
            val bytebuf = buf.convertToNetty().asInstanceOf[ByteBuf]
            val len = bytebuf.readableBytes()
            assert(len % 100 == 0)
            assert(bytebuf.hasMemoryAddress)

            val start = bytebuf.memoryAddress + bytebuf.readerIndex
            UnsafeSort.UNSAFE.copyMemory(start, sortBuffer.address + offset, len)
            offset += len
            bytebuf.release()

          case buf: FileSegmentManagedBuffer =>
            val fs = new FileInputStream(buf.file)
            val channel = fs.getChannel
            channel.position(buf.offset)
            assert(buf.length < 4 * 1024 * 1024)
            sortBuffer.ioBuf.clear()
            sortBuffer.ioBuf.limit(buf.length.toInt)
            sortBuffer.setIoBufAddress(sortBuffer.address + offset)
            //var read0 = 0L
            //while (read0 < buf.length) {
              val read0 = channel.read(sortBuffer.ioBuf)
              //println(s"read $thisRead total $read0 buf size ${buf.length} ")
              //read0 += thisRead
            //}
            assert(read0 == buf.length)
            offset += read0
            channel.close()
            fs.close()
        }

        numShuffleBlocks += 1
      }

      val timeTaken = System.currentTimeMillis() - startTime
      logInfo(s"XXX Reduce: took $timeTaken ms to fetch $numShuffleBlocks shuffle blocks $offset bytes")
      println(s"XXX Reduce: took $timeTaken ms to fetch $numShuffleBlocks shuffle blocks $offset bytes")

      buildLongPointers(sortBuffer, offset)
      val pointers = sortBuffer.pointers

      val numRecords = (offset / 100).toInt

      // Sort!!!
      {
        val startTime = System.currentTimeMillis
        val sorter = new Sorter(new LongArraySorter).sort(
          sortBuffer.pointers, 0, numRecords, ord)
        val timeTaken = System.currentTimeMillis - startTime
        logInfo(s"XXX Reduce: Sorting $numRecords records took $timeTaken ms")
        println(s"XXX Reduce: Sorting $numRecords records took $timeTaken ms")
        scala.Console.flush()
      }

      val count: Long = {
        val startTime = System.currentTimeMillis

        // Pick the EBS volume to write to. We pick a random one hoping to balance out the writes.
        val volIndex = new java.util.Random().nextInt(NUM_EBS)
        val baseFolder = s"/vol$volIndex/sort-${sizeInGB}g-$numParts-out"
        if (!new File(baseFolder).exists()) {
          new File(baseFolder).mkdirs()
        }

        val outputFile = s"$baseFolder/part$part.dat"
        val os = new BufferedOutputStream(new FileOutputStream(outputFile), 4 * 1024 * 1024)
        val buf = new Array[Byte](100)
        val arrOffset = BYTE_ARRAY_BASE_OFFSET
        var i = 0
        while (i < numRecords) {
          val addr = pointers(i)
          UNSAFE.copyMemory(null, addr, buf, arrOffset, 100)
          os.write(buf)
          i += 1
        }
        os.close()
        val timeTaken = System.currentTimeMillis - startTime
        logInfo(s"XXX Reduce: writing $numRecords records took $timeTaken ms")
        println(s"XXX Reduce: writing $numRecords records took $timeTaken ms")
        i.toLong
      }
      Iterator(count)
    }.reduce(_ + _)

    println("total number of records: " + recordsAfterSort)
  }

  final val UNSAFE: sun.misc.Unsafe = {
    val unsafeField = classOf[sun.misc.Unsafe].getDeclaredField("theUnsafe")
    unsafeField.setAccessible(true)
    unsafeField.get().asInstanceOf[sun.misc.Unsafe]
  }

  final val BYTE_ARRAY_BASE_OFFSET: Long = UNSAFE.arrayBaseOffset(classOf[Array[Byte]])

  /**
   * A class to hold information needed to run sort within each partition.
   *
   * @param capacity number of records the buffer can support. Each record is 100 bytes.
   */
  final class SortBuffer(capacity: Long) {
    require(capacity <= Int.MaxValue)

    /** size of the buffer, starting at [[address]] */
    val len: Long = capacity * 100

    /** address pointing to a block of memory off heap */
    val address: Long = {
      val blockSize = capacity * 100
      logInfo(s"Allocating $blockSize bytes")
      val blockAddress = UNSAFE.allocateMemory(blockSize)
      logInfo(s"XXX Allocating $blockSize bytes ... allocated at $blockAddress")
      println(s"XXX Allocating $blockSize bytes ... allocated at $blockAddress")
      blockAddress
    }

    /**
     * A dummy direct buffer. We use this in a very unconventional way. We use reflection to
     * change the address of the offheap memory to our large buffer, and then use channel read
     * to directly read the data into our large buffer.
     *
     * i.e. the 4MB allocated here is not used at all. We are only the 4MB for tracking.
     */
    val ioBuf: ByteBuffer = ByteBuffer.allocateDirect(4 * 1024 * 1024)

    /** list of pointers to each block, used for sorting. */
    val pointers: Array[Long] = new Array[Long](capacity.toInt)

    private[this] val ioBufAddressField = {
      val f = classOf[java.nio.Buffer].getDeclaredField("address")
      f.setAccessible(true)
      f
    }

    /** Return the memory address of the memory the [[ioBuf]] points to. */
    def ioBufAddress: Long = ioBufAddressField.getLong(ioBuf)

    def setIoBufAddress(addr: Long) = {
      ioBufAddressField.setLong(ioBuf, addr)
    }
  }

  /** A thread local variable storing a pointer to the buffer allocated off-heap. */
  val sortBuffers = new ThreadLocal[SortBuffer]

  def readFileIntoBuffer(inputFile: String, sortBuffer: SortBuffer) {
    logInfo(s"reading file $inputFile")
    val startTime = System.currentTimeMillis()
    val fileSize = new File(inputFile).length
    assert(fileSize % 100 == 0)

    val baseAddress: Long = sortBuffer.address
    var is: FileInputStream = null
    var channel: FileChannel = null
    var read = 0L
    try {
      is = new FileInputStream(inputFile)
      channel = is.getChannel()
      while (read < fileSize) {
        // This should read read0 bytes directly into our buffer
        sortBuffer.setIoBufAddress(baseAddress + read)
        val read0 = channel.read(sortBuffer.ioBuf)
        //UNSAFE.copyMemory(sortBuffer.ioBufAddress, baseAddress + read, read0)
        sortBuffer.ioBuf.clear()
        read += read0
      }
    } finally {
      if (channel != null) {
        channel.close()
      }
      if (is != null) {
        is.close()
      }
    }
    val timeTaken = System.currentTimeMillis() - startTime
    logInfo(s"XXX finished reading file $inputFile ($read bytes), took $timeTaken ms")
    println(s"XXX finished reading file $inputFile ($read bytes), took $timeTaken ms")
    scala.Console.flush()
    assert(read == fileSize)
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

    val hosts = Sort.readSlaves()
    new NodeLocalRDD[(Long, Array[Long])](sc, numParts, hosts) {
      override def compute(split: Partition, context: TaskContext) = {
        val part = split.index
        val host = split.asInstanceOf[NodeLocalRDDPartition].node

        val start = recordsPerPartition * part
        val volIndex = part % NUM_EBS

        val baseFolder = s"/vol$volIndex/sort-${sizeInGB}g-$numParts"
        val outputFile = s"$baseFolder/part$part.dat"

        val fileSize = new File(outputFile).length
        assert(fileSize % 100 == 0)

        if (sortBuffers.get == null) {
          // Allocate 10% overhead since after shuffle the partitions can get slightly uneven.
          val capacity = recordsPerPartition + recordsPerPartition / 10
          sortBuffers.set(new SortBuffer(capacity))
        }

        val sortBuffer = sortBuffers.get()

        readFileIntoBuffer(outputFile, sortBuffer)
        buildLongPointers(sortBuffer, fileSize)

        // Sort!!!
        {
          val startTime = System.currentTimeMillis
          val sorter = new Sorter(new LongArraySorter).sort(
            sortBuffer.pointers, 0, recordsPerPartition.toInt, ord)
          val timeTaken = System.currentTimeMillis - startTime
          logInfo(s"XXX Sorting $recordsPerPartition records took $timeTaken ms")
          println(s"XXX Sorting $recordsPerPartition records took $timeTaken ms")
          scala.Console.flush()
        }

        Iterator((recordsPerPartition, sortBuffer.pointers))
      }
    }
  }


  private[spark]
  final class LongArraySorter extends SortDataFormat[Long, Array[Long]] {
    /** Return the sort key for the element at the given index. */
    override protected def getKey(data: Array[Long], pos: Int): Long = data(pos)

    /** Swap two elements. */
    override protected def swap(data: Array[Long], pos0: Int, pos1: Int) {
      val tmp = data(pos0)
      data(pos0) = data(pos1)
      data(pos1) = tmp
    }

    /** Copy a single element from src(srcPos) to dst(dstPos). */
    override protected def copyElement(src: Array[Long], srcPos: Int,
                                       dst: Array[Long], dstPos: Int) {
      dst(dstPos) = src(srcPos)
    }

    /**
     * Copy a range of elements starting at src(srcPos) to dst, starting at dstPos.
     * Overlapping ranges are allowed.
     */
    override protected def copyRange(src: Array[Long], srcPos: Int,
                                     dst: Array[Long], dstPos: Int, length: Int) {
      System.arraycopy(src, srcPos, dst, dstPos, length)
    }

    /**
     * Allocates a Buffer that can hold up to 'length' elements.
     * All elements of the buffer should be considered invalid until data is explicitly copied in.
     */
    override protected def allocate(length: Int): Array[Long] = new Array[Long](length)
  }

}
