package org.apache.spark.sort

import org.apache.spark.Partitioner


final class DaytonaPartitioner(rangeBounds: Array[Long]) extends Partitioner {

  private[this] var currentPart: Int = 0
  private[this] var currentHiKey: Long = 0L
  private[this] var currentLoKey: Long = 0L

  private[this] var keys: Array[Long] = null

  def setKeys(keys: Array[Long]) {
    this.keys = keys
    currentPart = 0
    currentHiKey = keys(0)
    currentLoKey = keys(1)
  }

  override def numPartitions: Int = rangeBounds.length

  override def getPartition(key: Any): Int = ???

  def getPartitionSpecialized(key1: Long, key2: Long): Int = {
    if (currentPart == keys.length - 1) {
      return currentPart
    } else {
      val c1 = java.lang.Long.compare(key1, currentHiKey)
      if (c1 < 0) {
        return currentPart
      } else if (c1 == 0) {
        val c2 = java.lang.Long.compare(key1, currentHiKey)
        if (c2 <= 0) {
          return currentPart
        }
      }
    }
    currentPart += 1
    currentHiKey = keys(currentPart * 2)
    currentLoKey = keys(currentPart * 2 + 1)
    return currentPart
  }
}
