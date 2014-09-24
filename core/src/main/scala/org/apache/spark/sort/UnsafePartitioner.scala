package org.apache.spark.sort

import com.google.common.primitives.Longs
import org.apache.spark.Partitioner


case class UnsafePartitioner(numPartitions: Int) extends Partitioner {
  import UnsafePartitioner._

  private[this] val rangePerPart: Long = {
    val range = max - min + 1
    val mod = range % numPartitions
    val r = if (mod == 0) {
      range / numPartitions
    } else {
      range / numPartitions + 1
    }
    r
  }

  /**
   * Get the partition ID of a record (which contains an address pointing to an off-heap buffer.
   *
   * This works by getting a long value (8 bytes) from the beginning of the record, reverse the
   * bytes (since x86 is little-endian, and we want unsigned bytes comparison), and then shift
   * to the right 1 byte.
   *
   * This should be functionally equivalent to getting the first 7 bytes and assemble a long if
   * the architecture is little-endian.
   *
   * As an example, input: 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a.
   * getLong: 0x80 x07 0x06 0x05 0x04 0x03 0x02 0x01
   * reverseBytes: 0x01 0x02 0x03 0x04 0x05 0x06 0x07 0x08
   * >>> 8: 0x00 0x01 0x02 0x03 0x04 0x05 0x06 0x07
   */
  override def getPartition(key: Any): Int = {
    val addr: Long = key.asInstanceOf[Long]
    val prefix = SortUtils.UNSAFE.getLong(addr)
    ((java.lang.Long.reverseBytes(prefix) >>> 8) / rangePerPart).toInt
  }
}

object UnsafePartitioner {

  val min = Longs.fromBytes(0, 0, 0, 0, 0, 0, 0, 0)
  val max = Longs.fromBytes(0, -1, -1, -1, -1, -1, -1, -1)  // 0xff = -1

  /** Test code */
  def main(args: Array[String]) {
    val addr = SortUtils.UNSAFE.allocateMemory(10)
    val bytes: Array[Byte] = (1 to 10).map(_.toByte).toArray
    assert(bytes.length == 10)

    println("add is " + addr)
    SortUtils.UNSAFE.copyMemory(bytes, SortUtils.BYTE_ARRAY_BASE_OFFSET, null, addr, 10L)

    val prefix = SortUtils.UNSAFE.getLong(addr)
    println(java.lang.Long.toHexString(java.lang.Long.reverseBytes(prefix) >>> 8))

    println("--------------------------------------")
    testArray(2, Array[Byte](-1, -1, -1, -1, -1, -1, -1, -1))
    testArray(2, Array[Byte](-1, -1, -1, -1, -1, -1, -1, 0))
    testArray(2, Array[Byte](0, -1, -1, -1, -1, -1, -1, 0))
  }

  private def testArray(numPart: Int, in: Array[Byte]) {
    val aPt = createOffHeapBuf(in)
    println(s"${in.toSeq} " + new UnsafePartitioner(numPart).getPartition(aPt))
  }

  private def createOffHeapBuf(bytes: Array[Byte]): Long = {
    val addr = SortUtils.UNSAFE.allocateMemory(bytes.length)
    SortUtils.UNSAFE.copyMemory(bytes, SortUtils.BYTE_ARRAY_BASE_OFFSET, null, addr, bytes.length)
    addr
  }
}
