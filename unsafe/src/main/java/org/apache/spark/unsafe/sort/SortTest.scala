package org.apache.spark.unsafe.sort

object SortTest {

  def main(args: Array[String]): Unit = {
    val size = 1 << args(0).toInt
    val input = new ChunkedArray(RadixSort.NUM_WORDS_PER_CHUNK, 2)
    var i = 0
    while (i < size) {
      input.append(scala.util.Random.nextLong(), scala.util.Random.nextLong())
      i += 1
    }

    for (i <- 0 until 5) {
      val start = System.nanoTime()
      RadixSort.sort(input)
      val end = System.nanoTime()
      println(s"#$i : ${(end - start) / 1e9}")
    }
  }
}
