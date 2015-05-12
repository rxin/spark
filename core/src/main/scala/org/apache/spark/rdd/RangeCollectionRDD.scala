/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.rdd

import scala.collection.Map

import org.apache.spark._

private[spark] class RangeCollectionRDD(
    @transient sc: SparkContext,
    limit: Long,
    numSlices: Int,
    locationPrefs: Map[Int, Seq[String]])
  extends RDD[Long](sc, Nil) {

  override def getPartitions: Array[Partition] = {
    val slices = RangeCollectionRDD.sliceIntoIterator(limit, numSlices).toArray
    slices.indices.map(i => new ParallelCollectionPartition(id, i, slices(i).toArray.toSeq)).toArray
  }

  override def compute(s: Partition, context: TaskContext): Iterator[Long] = {
    new InterruptibleIterator(context, s.asInstanceOf[ParallelCollectionPartition[Long]].iterator)
  }

  override def getPreferredLocations(s: Partition): Seq[String] = {
    locationPrefs.getOrElse(s.index, Nil)
  }
}

private object RangeCollectionRDD {
  def sliceIntoIterator(limit: Long, numSlices: Int): Seq[Iterator[Long]] = {
    (0 until numSlices).map(i => {
      val start = (i * limit) / numSlices
      val end = ((i + 1) * limit) / numSlices

      new Iterator[Long] {
        var number: Long = _
        initialize()

        override def hasNext = number < end

        override def next() = {
          val ret = number
          number += 1
          ret
        }

        private def initialize() = {
          number = start
        }
      }
    })
  }
}
