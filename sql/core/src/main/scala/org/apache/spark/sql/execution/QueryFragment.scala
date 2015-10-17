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
import org.apache.spark.sql.catalyst.rules.{Rule, RuleExecutor}

case class QueryFragment(subplan: SparkPlan, children: Seq[QueryFragment]) extends SparkPlan {

  override def output: Seq[Attribute] = subplan.output

  override def isLocal: Boolean = true

  override protected def doExecute(): RDD[InternalRow] = {
    if (subplan.isLocal) {
      val inputRDDs = children.map(_.execute())

      def run(iters: Map[Long, Iterator[InternalRow]]): Iterator[InternalRow] = {
        null
      }

      new CoPartitionedRDD(sparkContext, run, children, inputRDDs.toArray)
    } else {
      subplan.execute()
    }
  }

  override def simpleString: String = "QueryFragment: " + subplan
}


object QueryFragmentBuilder {

  def execute(plan: SparkPlan): SparkPlan = {
    rule.execute(RootNode(plan)) match {
      case RootNode(child) => child
      case root => root
    }
  }

  private val rule = new RuleExecutor[SparkPlan] {
    override protected val batches: Seq[Batch] =
      Batch("BuildFragment", FixedPoint(100), BuildFragment) :: Nil

    object BuildFragment extends Rule[SparkPlan] {
      def apply(plan: SparkPlan): SparkPlan = plan transformUp {
        case RootNode(node) =>
          val children = node.collectFrontier { case fragment: QueryFragment => fragment }
          QueryFragment(node, children)

        case node if !node.isLocal =>
          val children = node.collectFrontier { case fragment: QueryFragment => fragment }
          QueryFragment(node, children)
      }
    }
  }

  private case class RootNode(child: SparkPlan) extends UnaryNode {
    override protected def doExecute(): RDD[InternalRow] = throw new UnsupportedOperationException
    override def output: Seq[Attribute] = child.output
  }
}
