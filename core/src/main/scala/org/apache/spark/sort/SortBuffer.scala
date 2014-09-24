package org.apache.spark.sort

import java.nio.ByteBuffer

import org.apache.spark.sort.IndySort._

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
  val address: Long = {
    val blockSize = capacity * 100
    val blockAddress = UNSAFE.allocateMemory(blockSize)
    blockAddress
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
