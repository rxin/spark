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

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.{CatalystTypeConverters, InternalRow}
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.trees.TreeNode
import org.apache.spark.sql.types.StructType


abstract class LocalNode extends TreeNode[LocalNode] {

  def output: Seq[Attribute]

  def open(): Unit

  def next(): Boolean

  def get(): InternalRow

  def close(): Unit

  def collect(): Seq[Row] = {
    val converter = CatalystTypeConverters.createToScalaConverter(StructType.fromAttributes(output))
    val result = new ArrayBuffer[Row]
    open()
    while (next()) {
      result += converter.apply(get()).asInstanceOf[Row]
    }
    close()
    result
  }
}


abstract class LeafLocalNode extends LocalNode {
  override def children: Seq[LocalNode] = Seq.empty
}


abstract class UnaryLocalNode extends LocalNode {

  def child: LocalNode

  override def children: Seq[LocalNode] = Seq(child)
}
