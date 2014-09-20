package org.apache.spark.sort.old

import org.apache.spark.sort.UnsafeSort

/**
 * Sort ordering for comparing 10-byte arrays.
 *
 * http://grepcode.com/file/repo1.maven.org/maven2/com.google.guava/guava/17.0/com/google/common/primitives/UnsignedBytes.java#298
 */
class TeraSortOrdering extends Ordering[Array[Byte]] {
  import org.apache.spark.sort.old.TeraSortOrdering._

  override def compare(left: Array[Byte], right: Array[Byte]): Int = {
    val lw: Long = UNSAFE.getLong(left, BYTE_ARRAY_BASE_OFFSET)
    val rw: Long = UNSAFE.getLong(right, BYTE_ARRAY_BASE_OFFSET)
    if (lw != rw) {
      val n: Int = java.lang.Long.numberOfTrailingZeros(lw ^ rw) & ~0x7
      (((lw >>> n) & UNSIGNED_MASK) - ((rw >>> n) & UNSIGNED_MASK)).asInstanceOf[Int]
    } else {
      // First word not equal. Compare the rest 2 bytes.
      val diff9 = (left(8) & UNSIGNED_MASK) - (right(8) & UNSIGNED_MASK)
      if (diff9 != 0) {
        diff9
      } else {
        (left(9) & UNSIGNED_MASK) - (right(9) & UNSIGNED_MASK)
      }
    }
  }
}


object TeraSortOrdering {
  private final val UNSIGNED_MASK: Int = 0xFF
  private final val UNSAFE: sun.misc.Unsafe = UnsafeSort.UNSAFE
  private final val BYTE_ARRAY_BASE_OFFSET: Long = UnsafeSort.BYTE_ARRAY_BASE_OFFSET
}
