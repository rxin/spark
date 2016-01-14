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

package org.apache.spark.sql.execution

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.execution.local.{IteratorScanNode, LocalNode}


case class QueryFragment(terminalNode: LocalNode, children: Seq[SparkPlan]) extends SparkPlan {

  override protected def doExecute(): RDD[InternalRow] = {
    if (children.size == 0) {
      throw UnsupportedOperationException
    } else {
      val inputRDDs = children.map(_.execute())
      val sc = inputRDDs.head.sparkContext
      new CoPartitionedRDD(sc, inputRDDs, { iters: Seq[Iterator[InternalRow]] =>
        val leafs = terminalNode.collect {case n: IteratorScanNode => n}
        leafs.zip(iters).map {case (leaf, iter) => leaf.withIterator (iter)}
        terminalNode.prepare ()
        terminalNode.open ()
        terminalNode.asIterator
      })
    }
  }

  override def output: Seq[Attribute] = terminalNode.output
}
