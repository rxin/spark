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

package org.apache.spark.sql

import scala.language.implicitConversions

import org.apache.spark.rdd.RDD
import org.apache.spark.api.java.function.Function
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.{JoinType, Inner}
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.execution.{SparkPlan, ExtractPythonUdfs}
import org.apache.spark.sql.types.{NumericType, StructType}


class DataFrame(
    val sqlContext: SQLContext,
    val plan: LogicalPlan,
    operatorsEnabled: Boolean)
  extends DataFrameSpecificApi with RDDApi[Row] {

  def this(sqlContext: Option[SQLContext], plan: Option[LogicalPlan]) =
    this(sqlContext.orNull, plan.orNull, sqlContext.isDefined && plan.isDefined)

  def this(sqlContext: SQLContext, plan: LogicalPlan) = this(sqlContext, plan, true)

  protected[sql] lazy val analyzed: LogicalPlan = ExtractPythonUdfs(sqlContext.analyzer(plan))
  protected[sql] lazy val withCachedData = sqlContext.useCachedData(analyzed)
  protected[sql] lazy val optimizedPlan = sqlContext.optimizer(withCachedData)

  private[this] implicit def toDataFrame(logicalPlan: LogicalPlan): DataFrame = {
    new DataFrame(sqlContext, logicalPlan, true)
  }

  // TODO: Don't just pick the first one...
  protected[sql] lazy val sparkPlan = {
    SparkPlan.currentContext.set(sqlContext)
    sqlContext.planner(optimizedPlan).next()
  }
  // executedPlan should not be used to initialize any SparkPlan. It should be
  // only used for execution.
  protected[sql] lazy val executedPlan: SparkPlan = sqlContext.prepareForExecution(sparkPlan)

  protected[sql] def numericColumns: Seq[Expression] = {
    schema.fields.filter(_.dataType.isInstanceOf[NumericType]).map { n =>
      plan.resolve(n.name, sqlContext.analyzer.resolver).get
    }
  }

  override def rdd: RDD[Row] = executedPlan.execute()

  override def schema: StructType = analyzed.schema

  override def dtypes: Seq[(String, String)] = schema.fields.map { field =>
    (field.name, field.dataType.toString)
  }

  override def columns: Seq[String] = schema.fields.map(_.name)

  override def join(right: DataFrame): DataFrame = {
    Join(plan, right.plan, joinType = Inner, None)
  }

  override def join(right: DataFrame, joinExprs: Column): DataFrame = {
    Join(plan, right.plan, Inner, Some(joinExprs.expr))
  }

  override def join(right: DataFrame, how: String, joinExprs: Column): DataFrame = {
    Join(plan, right.plan, JoinType(how), Some(joinExprs.expr))
  }

  override def sort(colName: String): DataFrame = {
    Sort(Seq(SortOrder(apply(colName).expr, Ascending)), global = true, plan)
  }

  override def sort(sortExprs: Column*): DataFrame = {
    val sortOrder: Seq[SortOrder] = sortExprs.map { col =>
      col.expr match {
        case expr: SortOrder =>
          expr
        case expr: Expression =>
          SortOrder(expr, Ascending)
      }
    }
    Sort(sortOrder, global = true, plan)
  }

  override def tail(n: Int): Array[Row] = ???

  /** Selecting a single column */
  override def apply(colName: String): Column = {
    val expr = plan.resolve(colName, sqlContext.analyzer.resolver).getOrElse(
      throw new RuntimeException(s"""Cannot resolve column name "$colName""""))
    new Column(Some(sqlContext), Some(Project(Seq(expr), plan)), expr)
  }

  /** Projection */
  override def apply(projection: Product): DataFrame = select(projection.productIterator.map {
    case c: Column => c
    case o: Any => new Column(Some(sqlContext), None, Literal(o))
  }.toSeq :_*)

  @scala.annotation.varargs
  override def select(cols: Column*): DataFrame = {
    val exprs = cols.zipWithIndex.map {
      case (Column(expr: NamedExpression), _) =>
        expr
      case (Column(expr: Expression), _) =>
        Alias(expr, expr.toString)()
    }
    Project(exprs.toSeq, plan)
  }

  /** Filtering */
  override def filter(condition: Column): DataFrame = {
    Filter(condition.expr, plan)
  }

  override def updateColumn(colName: String, col: Column): DataFrame = ???

  @scala.annotation.varargs
  override def groupby(cols: Column*): GroupedDataFrame = {
    new GroupedDataFrame(this, cols.map(_.expr))
  }

  /////////////////////////////////////////////////////////////////////////////
  override def addColumn(colName: String, col: Column): DataFrame = ???

  override def head(n: Int): Array[Row] = ???

  override def removeColumn(colName: String, col: Column): DataFrame = ???

  override def map[R](f: Row => R): RDDApi[R] = ???

  override def map[R](f: Function[Row, R]): RDDApi[R] = ???
}