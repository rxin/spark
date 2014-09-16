package org.apache.spark.sort

import com.google.common.primitives.UnsignedBytes


/**
 * Sort ordering for comparing 10-byte arrays for records from an off-heap block.
 *
 * http://grepcode.com/file/repo1.maven.org/maven2/com.google.guava/guava/17.0/com/google/common/primitives/UnsignedBytes.java#298
 */
class UnsafeOrdering extends Ordering[Long] {
  import UnsafeOrdering._

  override def compare(left: Long, right: Long): Int = {
    val lw: Long = UNSAFE.getLong(left)
    val rw: Long = UNSAFE.getLong(right)
    if (lw != rw) {
      val n: Int = java.lang.Long.numberOfTrailingZeros(lw ^ rw) & ~0x7
      (((lw >>> n) & UNSIGNED_MASK) - ((rw >>> n) & UNSIGNED_MASK)).asInstanceOf[Int]
    } else {
      // First word not equal. Compare the rest 2 bytes.
      val diff9 = (UNSAFE.getByte(left + 8) & UNSIGNED_MASK) - (UNSAFE.getByte(right + 8) & UNSIGNED_MASK)
      if (diff9 != 0) {
        diff9
      } else {
        (UNSAFE.getByte(left + 9) & UNSIGNED_MASK) - (UNSAFE.getByte(right + 9) & UNSIGNED_MASK)
      }
    }
  }
}


object UnsafeOrdering {
  private final val UNSIGNED_MASK: Int = 0xFF
  private final val UNSAFE: sun.misc.Unsafe = UnsafeSort.UNSAFE

  def main(args: Array[String]) {

    def assertAndPrint(expr: Int, expected: Int) {
      println(expr)
      assert(expr == expected)
    }

    val ord = new UnsafeOrdering
    val buf1 = createOffHeapBuf(Array[Byte](1, 2, 3, 4, 5, 6, 7, 8, 9, 10))
    val buf2 = createOffHeapBuf(Array[Byte](2, 2, 3, 4, 5, 6, 7, 8, 9, 10))
    val buf3 = createOffHeapBuf(Array[Byte](2, 2, 3, 4, 6, 6, 7, 8, 9, 10))
    val buf4 = createOffHeapBuf(Array[Byte](1, 2, 3, 4, 5, 6, 7, 8, 10, 10))
    val buf5 = createOffHeapBuf(Array[Byte](1, 2, 3, 4, 5, 6, 7, 8, 10, 11))
    val buf6 = createOffHeapBuf(Array[Byte](150.toByte, 2, 3, 4, 5, 6, 7, 8, 10, 11))
    assertAndPrint(ord.compare(buf1, buf1), 0)
    assertAndPrint(ord.compare(buf2, buf2), 0)
    assertAndPrint(ord.compare(buf3, buf3), 0)

    assertAndPrint(ord.compare(buf1, buf2), -1)
    assertAndPrint(ord.compare(buf2, buf3), -1)
    assertAndPrint(ord.compare(buf2, buf1), 1)
    assertAndPrint(ord.compare(buf3, buf2), 1)

    assertAndPrint(ord.compare(buf4, buf5), -1)
    assertAndPrint(ord.compare(buf5, buf4), 1)

    assert(ord.compare(buf5, buf6) < 0)
    assert(ord.compare(buf6, buf5) > 1)

    println("doing random testing")

    for (i <- 1 to 10000000) {
      val buf1 = createRandomBuffer()
      val buf2 = createRandomBuffer()
      val cmp = UnsignedBytes.lexicographicalComparator().compare(buf1, buf2)
      val got = ord.compare(createOffHeapBuf(buf1), createOffHeapBuf(buf2))
      if (got != cmp) {
        println(s"guava output $cmp, while we output $got")
        println("buf1: " + buf1.map(UnsignedBytes.toString(_)).toSeq)
        println("buf2: " + buf2.map(UnsignedBytes.toString(_)).toSeq)
        System.exit(1)
      } else {
        println(s"comparison $i passed guava output $cmp, while we output $got ")
      }
    }
  }

  private val rand = new scala.util.Random()

  private def createRandomBuffer(): Array[Byte] = {
    val a = new Array[Byte](10)
    rand.nextBytes(a)
    a
  }

  private def createOffHeapBuf(bytes: Array[Byte]): Long = {
    val addr = UNSAFE.allocateMemory(bytes.length)
    UNSAFE.copyMemory(bytes, UnsafeSort.BYTE_ARRAY_BASE_OFFSET, null, addr, bytes.length)
    addr
  }
}

