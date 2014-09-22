package org.apache.spark.sort

import scala.reflect.ClassTag

import org.apache.spark.rdd.RDD
import org.apache.spark.{Logging, SparkContext, Partition}


class NodeLocalRDDPartition(val index: Int, val node: String) extends Partition


abstract class NodeLocalRDD[T: ClassTag](
    sc: SparkContext, numParts: Int, @transient hosts: Array[String])
  extends RDD[T](sc, Nil) with Logging {

  override def getPreferredLocations(split: Partition): Seq[String] = {
    Seq(split.asInstanceOf[NodeLocalRDDPartition].node)
  }

  override protected def getPartitions: Array[Partition] = Array.tabulate(numParts) { i =>
    new NodeLocalRDDPartition(i, hosts(i % hosts.length))
  }
}


class NodeLocalReplicaRDDPartition(val index: Int, val node: Seq[String]) extends Partition


abstract class NodeLocalReplicaRDD[T: ClassTag](
    sc: SparkContext, numParts: Int, @transient hosts: Array[Seq[String]])
  extends RDD[T](sc, Nil) with Logging {

  override def getPreferredLocations(split: Partition): Seq[String] = {
    split.asInstanceOf[NodeLocalReplicaRDDPartition].node
  }

  override protected def getPartitions: Array[Partition] = Array.tabulate(numParts) { i =>
    new NodeLocalReplicaRDDPartition(i, hosts(i % hosts.length))
  }
}
