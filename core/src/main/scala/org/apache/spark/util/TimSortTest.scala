package org.apache.spark.util

object TimSortTest {

  def main(args: Array[String]): Unit = {
    val size = 1 << args(0).toInt
    val input = new Array[Long](size * 2)
    val longPairOrdering = new LongPairOrdering

    for (i <- 0 until 5) {
      var i = 0
      while (i < size) {
        input(i * 2) = scala.util.Random.nextLong()
        input(i * 2 + 1) = scala.util.Random.nextLong()
        i += 1
      }

      val start = System.nanoTime()
      new Sorter(new LongPairArraySorter).sort(input, 0, size, longPairOrdering)
      val end = System.nanoTime()
      println(s"#$i: ${(end - start) / 1e9}")
    }
  }
}


final class PairLong(var _1: Long, var _2: Long)

private[spark] trait SortDataFormat[K, Buffer] extends Any {
  /** Return the sort key for the element at the given index. */
  protected def getKey(data: Buffer, pos: Int): K

  protected def createNewMutableThingy(): K = null.asInstanceOf[K]

  protected def getKey(data: Buffer, pos: Int, mutableThingy: K): K = {
    getKey(data, pos)
  }

  /** Swap two elements. */
  protected def swap(data: Buffer, pos0: Int, pos1: Int): Unit

  /** Copy a single element from src(srcPos) to dst(dstPos). */
  protected def copyElement(src: Buffer, srcPos: Int, dst: Buffer, dstPos: Int): Unit

  /**
   * Copy a range of elements starting at src(srcPos) to dst, starting at dstPos.
   * Overlapping ranges are allowed.
   */
  protected def copyRange(src: Buffer, srcPos: Int, dst: Buffer, dstPos: Int, length: Int): Unit

  /**
   * Allocates a Buffer that can hold up to 'length' elements.
   * All elements of the buffer should be considered invalid until data is explicitly copied in.
   */
  protected def allocate(length: Int): Buffer
}

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
