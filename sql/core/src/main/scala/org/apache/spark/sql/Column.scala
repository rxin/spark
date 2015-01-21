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

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.logical.{Project, LogicalPlan}
import org.apache.spark.sql.types.DataType


object Column {
  def unapply(col: Column): Option[Expression] = Some(col.expr)
}


class Column(
    sqlContext: Option[SQLContext],
    plan: Option[LogicalPlan],
    val expr: Expression)
  extends DataFrame(sqlContext, plan) with ExpressionApi[Column] {

  private[this] implicit def toColumn(expr: Expression): Column = {
    val projectedPlan = plan.map { p =>
      Project(Seq(expr match {
        case named: NamedExpression => named
        case unnamed: Expression => Alias(unnamed, "col")()
      }), p)
    }
    new Column(sqlContext, projectedPlan, expr)
  }

  override def unary_- : Column = ???

  override def ||(other: Column): Column = ???

  override def unary_~ : Column = ???

  override def !==(other: Column): Column = ???

  override def >(other: Column): Column = ???

  override def unary_! : Column = ???

  override def &(other: Column): Column = ???

  override def /(other: Column): Column = ???

  override def &&(other: Column): Column = ???

  override def |(other: Column): Column = ???

  override def ^(other: Column): Column = ???

  override def <=>(other: Column): Column = ???

  override def ===(other: Column): Column = ???

  override def +(other: Column): Column = Add(expr, other.expr)

  def +(other: Any): Column = Add(expr, Literal(other))

  override def rlike(other: Column): Column = ???

  override def %(other: Column): Column = ???

  override def in(list: Column*): Column = ???

  override def getItem(ordinal: Column): Column = ???

  override def <=(other: Column): Column = ???

  override def like(other: Column): Column = ???

  override def getField(fieldName: String): Column = ???

  override def isNotNull: Column = ???

  override def substr(startPos: Column, len: Column): Column = ???

  override def <(other: Column): Column = ???

  override def isNull: Column = ???

  override def contains(other: Column): Column = ???

  override def -(other: Column): Column = ???

  override def desc: Column = ???

  override def >=(other: Column): Column = ???

  override def asc: Column = ???

  override def endsWith(other: Column): Column = ???

  override def *(other: Column): Column = ???

  override def startsWith(other: Column): Column = ???

  override def as(alias: String): Column = ???

  override def cast(to: DataType): Column = ???
}
