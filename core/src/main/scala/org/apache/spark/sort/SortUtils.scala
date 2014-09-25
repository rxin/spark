package org.apache.spark.sort

import java.nio.ByteBuffer

import org.apache.spark.util.collection.{SortDataFormat, Sorter}


object SortUtils {

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

    val IO_BUF_LEN = 4 * 1024 * 1024

    /** size of the buffer, starting at [[address]] */
    val len: Long = capacity * 100

    /** address pointing to a block of memory off heap */
    var address: Long = {
      val blockSize = capacity * 100
      val blockAddress = UNSAFE.allocateMemory(blockSize)
      blockAddress
    }

    def releaseMapSideBuffer() {
      if (address != 0) {
        UNSAFE.freeMemory(address)
        pointers = null
        address = 0L
      }
    }

    /**
     * Each chunk should be 512MB
     */
    val CHUNK_SIZE = 512L * 1000 * 1000
    val chunkBegin = new Array[Long](32)
    val chunkEnds = new Array[Long](32)
    var currentNumChunks = 0

    def currentChunkBaseAddress: Long = chunkBegin(currentNumChunks - 1)

    def allocateNewChunk() {
      println("allocating a new chunk at " + currentNumChunks)
      chunkBegin(currentNumChunks) = UNSAFE.allocateMemory(CHUNK_SIZE)
      currentNumChunks += 1
    }

    def markLastChunkUsage(len: Long) {
      assert(currentNumChunks > 0)
      println(s"mark $currentNumChunks usage at $len")
      chunkEnds(currentNumChunks - 1) = chunkBegin(currentNumChunks - 1) + len
    }

    def freeChunks() {
      var i = 0
      while (i < currentNumChunks) {
        UNSAFE.freeMemory(chunkBegin(i))
        i += 1
      }
      currentNumChunks = 0
    }

    /**
     * A dummy direct buffer. We use this in a very unconventional way. We use reflection to
     * change the address of the offheap memory to our large buffer, and then use channel read
     * to directly read the data into our large buffer.
     *
     * i.e. the 4MB allocated here is not used at all. We are only the 4MB for tracking.
     */
    val ioBuf: ByteBuffer = ByteBuffer.allocateDirect(IO_BUF_LEN)

    /** list of pointers to each block, used for sorting. */
    var pointers: Array[Long] = new Array[Long](capacity.toInt)

    /** an array of 2 * capacity longs that we can use for records holding our keys */
    var keys: Array[Long] = new Array[Long](2 * capacity.toInt)

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
  }  // end of SortBuffer

  /** A thread local variable storing a pointer to the buffer allocated off-heap. */
  val sortBuffers = new ThreadLocal[SortBuffer]

  def sortWithKeysUsingChunks(sortBuf: SortBuffer, numRecords: Int) {
    // Fill in the keys array
    sortBuf.keys = new Array[Long](numRecords * 2)
    val keys = sortBuf.keys
    var i = 0
    var chunkIndex = 0
    var indexWithinChunk = 0
    var addr: Long = sortBuf.chunkBegin(0)
    while (i < numRecords) {
      //assert(index >= 0L && index <= 0xFFFFFFFFL)
      if (addr >= sortBuf.chunkEnds(chunkIndex)) {
        chunkIndex += 1
        indexWithinChunk = 0
        addr = sortBuf.chunkBegin(chunkIndex)
      }
      val headBytes: Long = // First 7 bytes
        java.lang.Long.reverseBytes(UNSAFE.getLong(addr)) >>> 8
      val tailBytes: Long = // Last 3 bytes
        java.lang.Long.reverseBytes(UNSAFE.getLong(addr + 7)) >>> (8 * 5)
      keys(2 * i) = headBytes

      // Use the lower 23 bits for index within a chunk, and bit 23 to bit 31 for chunkIndex
      keys(2 * i + 1) = (tailBytes << 32) | (chunkIndex.toLong << 23) | indexWithinChunk.toLong
      addr += 100
      i += 1
      indexWithinChunk += 1
    }

    // Sort it
    new Sorter(new LongPairArraySorter).sort(keys, 0, numRecords, longPairOrdering)
  }

  // Sort a range of a SortBuffer using only the keys, then update the pointers field to match
  // sorted order. Unlike the other sort methods, this copies the keys into an array of Longs
  // (with 2 Longs per record in the buffer to capture the 10-byte key and its index) and sorts
  // them without having to look up random locations in the original data on each comparison.
  def sortWithKeys(sortBuf: SortBuffer, numRecords: Int) {
    val keys = sortBuf.keys
    val pointers = sortBuf.pointers
    val baseAddress = sortBuf.address
    var recordAddress = baseAddress
    import java.lang.Long.reverseBytes

    // Fill in the keys array
    var i = 0
    while (i < numRecords) {
      //assert(index >= 0L && index <= 0xFFFFFFFFL)
      val headBytes = // First 7 bytes
        reverseBytes(UNSAFE.getLong(recordAddress)) >>> 8
      val tailBytes = // Last 3 bytes
        reverseBytes(UNSAFE.getLong(recordAddress + 7)) >>> (8 * 5)
      keys(2 * i) = headBytes
      keys(2 * i + 1) = (tailBytes << 32) | i.toLong
      recordAddress += 100
      i += 1
    }

    // Sort it
    new Sorter(new LongPairArraySorter).sort(keys, 0, numRecords, longPairOrdering)

    // Fill back the pointers array
    i = 0
    while (i < numRecords) {
      pointers(i) = baseAddress + (keys(2 * i + 1) & 0xFFFFFFFFL) * 100
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

  final class PairLong(var _1: Long, var _2: Long)

  final class LongPairArraySorter extends SortDataFormat[PairLong, Array[Long]] {
    override protected def getKey(data: Array[Long], pos: Int) = ???

    override protected def createNewMutableThingy(): PairLong = new PairLong(0L, 0L)

    /** Return the sort key for the element at the given index. */
    override protected def getKey(data: Array[Long], pos: Int, reuse: PairLong): PairLong = {
      reuse._1 = data(2 * pos)
      reuse._2 = data(2 * pos + 1)
      reuse
      //(data(2 * pos), data(2 * pos + 1))
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

  final class LongPairOrdering extends Ordering[PairLong] {
    override def compare(left: PairLong, right: PairLong): Int = {
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
