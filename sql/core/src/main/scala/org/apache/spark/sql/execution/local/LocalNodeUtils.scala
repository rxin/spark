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

package org.apache.spark.sql.execution.local

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

import org.apache.spark.sql.execution.{SupportsLocalExecution, SparkPlan}

object LocalNodeUtils {

  def findLeafNodes(root: LocalNode): Seq[LocalNode] = {
    val result = new ArrayBuffer[LocalNode]
    val queue = new mutable.Queue[LocalNode]
    queue.enqueue(root)
    while (queue.nonEmpty) {
      val current = queue.dequeue()
      if (current.children.nonEmpty) {
        current.children.foreach(c => queue.enqueue(c))
      } else {
        result += current
      }
    }
    result
  }

  def findFrontierNonLocalNodes(root: SparkPlan): Seq[SparkPlan] = {
    val result = new ArrayBuffer[SparkPlan]
    val queue = new mutable.Queue[SparkPlan]
    queue.enqueue(root)
    while (queue.nonEmpty) {
      val current = queue.dequeue()
      if (current.isInstanceOf[SupportsLocalExecution]) {
        current.children.foreach(c => queue.enqueue(c))
      } else {
        result += current
      }
    }
    result
  }
}
