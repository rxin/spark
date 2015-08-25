package org.apache.spark.unsafe.sort

import scala.collection.JavaConversions._
import scala.collection.mutable.ArrayBuffer

import org.scalatest.FunSuite

import org.apache.spark.unsafe.sort.ChunkedArray.ForeachFunction


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
    java.lang.Long.compare(left._2, right._2)
  }
}


class RadixSortSuite extends FunSuite {

  test("basic test") {

    val input = Seq(
      (0L, 45L),
      (35473456723L, 45453456723L),
      (23433456456L, 34567835345L)
    )

    val buffer = new ChunkedArray(RadixSort.NUM_WORDS_PER_CHUNK, 2)
    input.foreach { case (data1, data2) =>
      buffer.append(data1, data2)
    }
    val sorted = RadixSort.sort(buffer)

    val output = new ArrayBuffer[(Long, Long)](input.size)
    sorted.foreach { arr =>
      arr.foreach(new ForeachFunction() {
        override def apply(chunk: LongArray, index: Int): Unit = {
          output += Tuple2(chunk.get(index), chunk.get(index + 1))
        }
      })
    }

    assert(output === input.sortBy(_._2))
  }

  test("timing test") {
    val size = 1 << 24
    val input = new ChunkedArray(4096, 2)
    var i = 0
    while (i < size) {
      input.append(i, scala.util.Random.nextLong())
      i += 1
    }

    for (i <- 0 until 5) {
      val start = System.nanoTime()
      RadixSort.sort(input)
      val end = System.nanoTime()
      println(s"#$i: ${(end - start) / 1e9}")
    }
  }

  test("tim sort") {
    val size = 1 << 24
    val input = new Array[Long](size * 2)
    var i = 0
    while (i < size) {
      input(i * 2) = i
      input(i * 2 + 1) = scala.util.Random.nextLong()
      i += 1
    }
    val longPairOrdering = new LongPairOrdering

    for (i <- 0 until 5) {
      val start = System.nanoTime()
      new Sorter(new LongPairArraySorter).sort(input, 0, size, longPairOrdering)
      val end = System.nanoTime()
      println(s"#$i: ${(end - start) / 1e9}")
    }
  }

  ignore("jvm sort") {
    val size = 1 << 26
    val input = new Array[Long](size)
    var i = 0
    while (i < size) {
      //input(i * 2) = i
      input(i) = scala.util.Random.nextLong()
      i += 1
    }

    for (i <- 0 until 5) {
      val start = System.nanoTime()
      java.util.Arrays.sort(input)
      val end = System.nanoTime()
      println(s"#$i: ${(end - start) / 1e9}")
    }
  }
}
