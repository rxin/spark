package org.apache.spark.sort

import java.io._
import java.nio.ByteBuffer
import java.util.concurrent.atomic.AtomicInteger

import _root_.io.netty.buffer.ByteBuf
import org.apache.spark._
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
        //val sorter = new Sorter(new LongArraySorter).sort(sortBuffer.pointers, 0, numRecords, ord)
        sortWithKeys(sortBuffer, 0, numRecords)
        val timeTaken = System.currentTimeMillis - startTime
        logInfo(s"XXX Reduce: Sorting $numRecords records took $timeTaken ms $outputFile")
        println(s"XXX Reduce: Sorting $numRecords records took $timeTaken ms $outputFile")
        scala.Console.flush()
      }

      Iterator(numRecords.toLong)
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

    /** an array of 2 * capacity longs that we can use for records holding our keys */
    val keys: Array[Long] = new Array[Long](2 * capacity.toInt)

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
        sortWithKeys(sortBuffer, 0, recordsPerPartition.toInt)
        val timeTaken = System.currentTimeMillis - startTime
        logInfo(s"XXX Sorting $recordsPerPartition records took $timeTaken ms")
        println(s"XXX Sorting $recordsPerPartition records took $timeTaken ms")
        scala.Console.flush()
      }

      Iterator((recordsPerPartition, sortBuffer.pointers))
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

  // Sort a range of a SortBuffer using only the keys, then update the pointers field to match
  // sorted order. Unlike the other sort methods, this copies the keys into an array of Longs
  // (with 2 Longs per record in the buffer to capture the 10-byte key and its index) and sorts
  // them without having to look up random locations in the original data on each comparison.
  private def sortWithKeys(sortBuf: SortBuffer, start: Int, end: Int) {
    val keys = sortBuf.keys
    val pointers = sortBuf.pointers
    val baseAddress = sortBuf.address
    import java.lang.Long.reverseBytes

    // Fill in the keys array
    var i = 0
    while (i < end - start) {
      val index = (pointers(start + i) - baseAddress) / 100
      //assert(index >= 0L && index <= 0xFFFFFFFFL)
      val headBytes = // First 7 bytes
        reverseBytes(UNSAFE.getLong(pointers(start + i))) >>> 8
      val tailBytes = // Last 3 bytes
        reverseBytes(UNSAFE.getLong(pointers(start + i) + 7)) >>> (8 * 5)
      keys(2 * i) = headBytes
      keys(2 * i + 1) = (tailBytes << 32) | index
      i += 1
    }

    // Sort it
    new Sorter(new LongPairArraySorter).sort(keys, 0, end - start, longPairOrdering)

    // Fill back the pointers array
    i = 0
    while (i < end - start) {
      pointers(start + i) = baseAddress + (keys(2 * i + 1) & 0xFFFFFFFFL) * 100
      i += 1
    }

    /*
    // Validate that the data is sorted
    i = start
    while (i < end - 1) {
      assert(ord.compare(pointers(i), pointers(i + 1)) <= 0)
      i += 1
    }
    */
  }

  final class LongPairArraySorter extends SortDataFormat[(Long, Long), Array[Long]] {
    /** Return the sort key for the element at the given index. */
    override protected def getKey(data: Array[Long], pos: Int): (Long, Long) = {
      (data(2 * pos), data(2 * pos + 1))
    }

    /** Swap two elements. */
    override protected def swap(data: Array[Long], pos0: Int, pos1: Int) {
      var tmp = data(2 * pos0)
      data(2 * pos0) = data(2 * pos1)
      data(2 * pos1) = tmp
      tmp = data(2 * pos0 + 1)
      data(2 * pos0 + 1) = data(2 * pos1 + 1)
      data(2 * pos1 + 1) = tmp
    }

    /** Copy a single element from src(srcPos) to dst(dstPos). */
    override protected def copyElement(src: Array[Long], srcPos: Int,
                                       dst: Array[Long], dstPos: Int) {
      dst(2 * dstPos) = src(2 * srcPos)
      dst(2 * dstPos + 1) = src(2 * srcPos + 1)
    }

    /**
     * Copy a range of elements starting at src(srcPos) to dst, starting at dstPos.
     * Overlapping ranges are allowed.
     */
    override protected def copyRange(src: Array[Long], srcPos: Int,
                                     dst: Array[Long], dstPos: Int, length: Int) {
      System.arraycopy(src, 2 * srcPos, dst, 2 * dstPos, 2 * length)
    }

    /**
     * Allocates a Buffer that can hold up to 'length' elements.
     * All elements of the buffer should be considered invalid until data is explicitly copied in.
     */
    override protected def allocate(length: Int): Array[Long] = new Array[Long](2 * length)
  }

  final class LongPairOrdering extends Ordering[(Long, Long)] {
    override def compare(left: (Long, Long), right: (Long, Long)): Int = {
      val c1 = java.lang.Long.compare(left._1, right._1)
      if (c1 != 0) {
        c1
      } else {
        java.lang.Long.compare(left._2, right._2)
      }
    }
  }

  val longPairOrdering = new LongPairOrdering

}
