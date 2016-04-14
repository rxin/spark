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

package org.apache.spark.accumulator

import java.{lang => jl}
import java.io.{ObjectInputStream, Serializable}
import java.util.concurrent.atomic.AtomicLong
import javax.annotation.concurrent.GuardedBy

import scala.collection.JavaConverters._

import org.apache.spark.TaskContext
import org.apache.spark.scheduler.AccumulableInfo
import org.apache.spark.util.Utils

/**
 * 1. internal accums can be created on the executors
2. internal accums are not registered with singleton object `Accumulators`
3. there are a fixed set of internal accums associated with each task
4. when a task fails, we count internal accums' values only if `countFailedValues` is true
5. when a stage is resubmitted completely, forget about all old internal accum values

[2:07]
for (4), external accumulators never set `countFailedValues` to true
for (5), external accumulators aren't affected by stage retries; as long as a successful task
updates the accum value, then the value is there forever
 */
class AccumulatorMetadata(
  val id: Long,
  val name: Option[String],
  val countFailedValues: Boolean
) extends Serializable


abstract class NewAccumulator[IN, OUT] extends Serializable {

  private[this] var metadata: AccumulatorMetadata = _

  def registerAccumulatorOnDriver(metadata: AccumulatorMetadata): Unit {
    this.metadata = metadata
  }

  def id: Long = metadata.id

  def reset(): Unit

  def add(v: IN): Unit

  def merge(other: NewAccumulator[IN, OUT]): Unit

  def value: OUT

  def localValue: OUT = value

  final def registerAccumulatorOnExecutor(): Unit = {
    // Automatically register the accumulator when it is deserialized with the task closure.
    // This is for external accumulators and internal ones that do not represent task level
    // metrics, e.g. internal SQL metrics, which are per-operator.
    val taskContext = TaskContext.get()
    if (taskContext != null) {
      taskContext.registerAccumulator(this)
    }
  }

  // Called by Java when deserializing an object
  private def readObject(in: ObjectInputStream): Unit = Utils.tryOrIOException {
    in.defaultReadObject()
    registerAccumulator()
  }

  private[spark] def toInfo(update: Option[Any], value: Option[Any]): AccumulableInfo = {
    new AccumulableInfo(
      metadata.id, metadata.name, update, value, metadata.internal, metadata.countFailedValues)
  }
}


object AccumulatorContext {

  /**
   * This global map holds the original accumulator objects that are created on the driver.
   * It keeps weak references to these objects so that accumulators can be garbage-collected
   * once the RDDs and user-code that reference them are cleaned up.
   * TODO: Don't use a global map; these should be tied to a SparkContext (SPARK-13051).
   */
  @GuardedBy("AccumulatorContext")
  private val originals = new java.util.HashMap[Long, jl.ref.WeakReference[NewAccumulator[_, _]]]

  private[this] val nextId = new AtomicLong(0L)

  /**
   * Return a globally unique ID for a new [[NewAccumulator]].
   * Note: Once you copy the [[NewAccumulator]] the ID is no longer unique.
   */
  def newId(): Long = nextId.getAndIncrement

  /**
   * Register an [[NewAccumulator]] created on the driver such that it can be used on the executors.
   *
   * All accumulators registered here can later be used as a container for accumulating partial
   * values across multiple tasks. This is what [[org.apache.spark.scheduler.DAGScheduler]] does.
   * Note: if an accumulator is registered here, it should also be registered with the active
   * context cleaner for cleanup so as to avoid memory leaks.
   *
   * If an [[NewAccumulator]] with the same ID was already registered, this does nothing instead
   * of overwriting it. This happens when we copy accumulators, e.g. when we reconstruct
   * [[org.apache.spark.executor.TaskMetrics]] from accumulator updates.
   */
  def register(a: NewAccumulator[_, _]): Unit = synchronized {
    if (!originals.containsKey(a.id)) {
      originals.put(a.id, new jl.ref.WeakReference[NewAccumulator[_, _]](a))
    }
  }

  /**
   * Unregister the [[NewAccumulator]] with the given ID, if any.
   */
  def remove(id: Long): Unit = synchronized {
    originals.remove(id)
  }

  /**
   * Return the [[NewAccumulator]] registered with the given ID, if any.
   */
  def get(id: Long): Option[NewAccumulator[_, _]] = synchronized {
    Option(originals.get(id)).map { ref =>
      // Since we are storing weak references, we must check whether the underlying data is valid.
      val acc = ref.get
      if (acc eq null) {
        throw new IllegalAccessError(s"Attempted to access garbage collected accumulator $id")
      }
      acc
    }
  }

  /**
   * Clear all registered [[NewAccumulator]]s. For testing only.
   */
  def clear(): Unit = synchronized {
    originals.clear()
  }
}


class LongAccumulator extends NewAccumulator[jl.Long, jl.Long] {
  private[this] var _sum = 0L

  override def add(v: jl.Long): Unit = {
    _sum += v
  }

  override def merge(other: NewAccumulator[jl.Long, jl.Long]): Unit = other match {
    case o: LongAccumulator => _sum += o.sum
    case _ => throw new UnsupportedOperationException(
      s"Cannot merge ${this.getClass.getName} with ${other.getClass.getName}")
  }

  override def value: jl.Long = _sum

  def sum: Long = _sum
}


class DoubleAccumulator extends NewAccumulator[jl.Double, jl.Double] {
  private[this] var _sum = 0.0

  override def add(v: jl.Double): Unit = {
    _sum += v
  }

  override def merge(other: NewAccumulator[jl.Double, jl.Double]): Unit = other match {
    case o: DoubleAccumulator => _sum += o.sum
    case _ => throw new UnsupportedOperationException(
      s"Cannot merge ${this.getClass.getName} with ${other.getClass.getName}")
  }

  override def value: jl.Double = _sum

  def sum: Double = _sum
}


class AverageAccumulator extends NewAccumulator[jl.Double, jl.Double] {
  private[this] var _sum = 0.0
  private[this] var _count = 0L

  override def add(v: jl.Double): Unit = {
    _sum += v
    _count += 1
  }

  override def merge(other: NewAccumulator[jl.Double, jl.Double]): Unit = other match {
    case o: AverageAccumulator =>
      _sum += o.sum
      _count += o.count
    case _ => throw new UnsupportedOperationException(
      s"Cannot merge ${this.getClass.getName} with ${other.getClass.getName}")
  }

  override def value: jl.Double = _sum

  def sum: Double = _sum

  def count: Long = _count
}


class CollectionAccumulator[T] extends NewAccumulator[T, java.util.List[T]] {
  private[this] val _list = new java.util.ArrayList[T]

  override def add(v: T): Unit = {
    _list.add(v)
  }

  override def merge(other: NewAccumulator[T, java.util.List[T]]): Unit = other match {
    case o: CollectionAccumulator[T] => _list.addAll(o.asList)
    case _ => throw new UnsupportedOperationException(
      s"Cannot merge ${this.getClass.getName} with ${other.getClass.getName}")
  }

  override def value: java.util.List[T] = asList

  def asList: java.util.List[T] = java.util.Collections.unmodifiableList(_list)

  def asSeq: Seq[T] = _list.asScala
}


class LegacyAccumulatorWrapper[R, T](param: org.apache.spark.AccumulableParam[R, T])
  extends NewAccumulator[T, R] {

  private[this] var _value: R = _

  def setInitialValue(initialValue: R): Unit = {
    _value = initialValue
  }

  override def add(v: T): Unit = {
    _value = param.addAccumulator(_value, v)
  }

  override def merge(other: NewAccumulator[T, R]): Unit = {
    _value = param.addInPlace(_value, other.value)
  }

  override def value: R = _value
}
