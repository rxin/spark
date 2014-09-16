package org.apache.spark.sort


/**
 * Sort ordering for comparing 10-byte arrays for records from an off-heap block.
 *
 * http://grepcode.com/file/repo1.maven.org/maven2/com.google.guava/guava/17.0/com/google/common/primitives/UnsignedBytes.java#298
 */
class UnsafeTeraSortOrdering extends Ordering[Long] {
  import UnsafeTeraSortOrdering._

  override def compare(left: Long, right: Long): Int = {
    val lw: Long = UNSAFE.getLong(left, left)
    val rw: Long = UNSAFE.getLong(right, right)
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


object UnsafeTeraSortOrdering {
  private final val UNSIGNED_MASK: Int = 0xFF
  private final val UNSAFE: sun.misc.Unsafe = UnsafeSort.UNSAFE
}
