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

import org.apache.spark.sql.catalyst.planning.{GenericStrategy, QueryPlanner}
import org.apache.spark.sql.catalyst.plans.logical
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan

class LocalPlanner extends QueryPlanner[LocalNode] {

  /** A list of execution strategies that can be used by the planner */
  override def strategies: Seq[GenericStrategy[LocalNode]] =
    BasicOperators :: Nil

  object BasicOperators extends GenericStrategy[LocalNode] {
    override def apply(plan: LogicalPlan): Seq[LocalNode] = plan match {

      case logical.LocalRelation(output, data) =>
        SeqScanNode(output, data) :: Nil

      case logical.Project(projectList, child) =>
        ProjectNode(projectList, planLater(child)) :: Nil

      case logical.Filter(condition, child) =>
        FilterNode(condition, planLater(child)) :: Nil
    }
  }
}
