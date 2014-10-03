package org.apache.spark.sort

import org.apache.spark.Partitioner


final class DaytonaPartitioner(rangeBounds: Array[Long]) extends Partitioner {

  private[this] var currentPart: Int = 0
  private[this] var currentHiKey: Long = 0L
  private[this] var currentLoKey: Long = 0L

  private[this] val lastPart: Int = rangeBounds.length / 2

  def setKeys() {
    currentPart = 0
    currentHiKey = rangeBounds(0)
    currentLoKey = rangeBounds(1)
  }

  override def numPartitions: Int = rangeBounds.length / 2 + 1

  override def getPartition(key: Any): Int = ???

  def getPartitionSpecialized(key1: Long, key2: Long): Int = {
    while (currentPart <= lastPart) {
      if (currentPart == lastPart) {
        return lastPart
      } else {
        val c1 = java.lang.Long.compare(key1, currentHiKey)
        if (c1 < 0) {
          return currentPart
        } else if (c1 == 0) {
          val c2 = java.lang.Long.compare(key2, currentLoKey)
          if (c2 <= 0) {
            return currentPart
          }
        }
      }
      currentPart += 1
      if (currentPart < lastPart) {
        currentHiKey = rangeBounds(currentPart * 2)
        currentLoKey = rangeBounds(currentPart * 2 + 1)
      }
    }
    assert(false, "something is wrong!!!!!!!!!!!!")
    return currentPart
  }
}




final class DaytonaPartitionerSkew(rangeBounds: Array[Long]) extends Partitioner {

  private[this] val inclusive = new Array[Boolean](rangeBounds.length / 2)

  private[this] var currentPart: Int = 0

  private[this] var prevHiKey: Long = -1L
  private[this] var prevLoKey: Long = -1L

  private[this] var currentBoundHiKey: Long = 0L
  private[this] var currentBoundLoKey: Long = 0L

  private[this] val lastPart: Int = rangeBounds.length / 2

  def setKeys() {
    currentPart = 0
    currentBoundHiKey = rangeBounds(0)
    currentBoundLoKey = rangeBounds(1)
  }

  override def numPartitions: Int = rangeBounds.length / 2 + 1

  override def getPartition(key: Any): Int = ???

  def getPartitionSpecialized(key1: Long, key2: Long): Int = {
    if (key1 == prevHiKey && key2 == prevLoKey) {
      return currentPart
    } else {
      prevHiKey = key1
      prevLoKey = key2
      while (currentPart <= lastPart) {
        if (currentPart == lastPart) {
          return lastPart
        } else {
          val c1 = java.lang.Long.compare(key1, currentBoundHiKey)
          if (c1 < 0) {
            return currentPart
          } else if (c1 == 0) {
            val c2 = java.lang.Long.compare(key2, currentBoundLoKey)
            if (c2 < 0) {
              return currentPart
            } else if (c2 == 0) {
              val nextBoundHikey = rangeBounds((currentPart + 1) * 2)
              val nextBoundLokey = rangeBounds((currentPart + 1) * 2 + 1)
              if (nextBoundHikey == currentBoundHiKey && nextBoundLokey == currentBoundLoKey) {
                currentPart += 1
                currentBoundHiKey = rangeBounds(currentPart * 2)
                currentBoundLoKey = rangeBounds(currentPart * 2 + 1)
                return currentPart
              }
            }
          }
        }
        currentPart += 1
        if (currentPart < lastPart) {
          currentBoundHiKey = rangeBounds(currentPart * 2)
          currentBoundLoKey = rangeBounds(currentPart * 2 + 1)
        }
      }
      assert(false, "something is wrong!!!!!!!!!!!!")
      return currentPart
    }
  }
}
