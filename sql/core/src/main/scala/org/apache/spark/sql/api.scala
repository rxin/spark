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

import org.apache.spark.api.java.function._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{DataType, StructType}


trait RDDApi[T] {

  def map[R](f: T => R): RDDApi[R]

  def map[R](f: Function[T, R]): RDDApi[R]

}


trait DataFrameSpecificApi {

  def rdd: RDD[Row]

  /**
   * Returns the schema of this SchemaRDD (represented by a [[StructType]]).
   *
   * @group schema
   */
  def schema: StructType

  /////////////////////////////////////////////////////////////////////////////
  // Metadata
  /////////////////////////////////////////////////////////////////////////////
  def dtypes: Seq[(String, String)]

  def columns: Seq[String]

  def head(n: Int = 5): Array[Row]

  def tail(n: Int = 5): Array[Row]

  /////////////////////////////////////////////////////////////////////////////
  // Relational operators
  /////////////////////////////////////////////////////////////////////////////
  /** Selecting a single column */
  def apply(colName: String): Column

  /** Projection */
  def apply(projection: Product): DataFrame

  @scala.annotation.varargs
  def select(cols: Column*): DataFrame

  /** Filtering */
  def apply(condition: Column): DataFrame = filter(condition)

  def filter(condition: Column): DataFrame

  def where(condition: Column): DataFrame = filter(condition)

  @scala.annotation.varargs
  def groupby(cols: Column*): GroupedDataFrame

  def sort(colName: String): DataFrame

  def sort(sortExprs: Column*): DataFrame

  def join(right: DataFrame): DataFrame

  def join(right: DataFrame, joinExprs: Column): DataFrame

  def join(right: DataFrame, how: String, joinExprs: Column): DataFrame

  /////////////////////////////////////////////////////////////////////////////
  // Column mutation
  /////////////////////////////////////////////////////////////////////////////
  def addColumn(colName: String, col: Column): DataFrame

  def removeColumn(colName: String, col: Column): DataFrame

  def updateColumn(colName: String, col: Column): DataFrame

  /////////////////////////////////////////////////////////////////////////////
  // Stat functions
  /////////////////////////////////////////////////////////////////////////////
//  def describe(): Unit
//
//  def mean(): Unit
//
//  def max(): Unit
//
//  def min(): Unit
}


trait ExpressionApi[ExprType] {
  def unary_- : ExprType
  def unary_! : ExprType
  def unary_~ : ExprType

  def + (other: ExprType): ExprType
  def - (other: ExprType): ExprType
  def * (other: ExprType): ExprType
  def / (other: ExprType): ExprType
  def % (other: ExprType): ExprType
  def & (other: ExprType): ExprType
  def | (other: ExprType): ExprType
  def ^ (other: ExprType): ExprType

  def && (other: ExprType): ExprType
  def || (other: ExprType): ExprType

  def < (other: ExprType): ExprType
  def <= (other: ExprType): ExprType
  def > (other: ExprType): ExprType
  def >= (other: ExprType): ExprType
  def === (other: ExprType): ExprType
  def <=> (other: ExprType): ExprType
  def !== (other: ExprType): ExprType

  def in(list: ExprType*): ExprType

  def like(other: ExprType): ExprType
  def rlike(other: ExprType): ExprType

  def contains(other: ExprType): ExprType
  def startsWith(other: ExprType): ExprType
  def endsWith(other: ExprType): ExprType

  def substr(startPos: ExprType, len: ExprType): ExprType
  def substring(startPos: ExprType, len: ExprType): ExprType = substr(startPos, len)

  def isNull: ExprType
  def isNotNull: ExprType

  def getItem(ordinal: ExprType): ExprType
  def getField(fieldName: String): ExprType

  def cast(to: DataType): ExprType

  def asc: ExprType
  def desc: ExprType

  def as(alias: String): ExprType
}


trait GroupedDataFrameApi {

  def agg(exprs: Map[String, String]): DataFrame

  def avg(): DataFrame

  def mean(): DataFrame

  def min(): DataFrame

  def max(): DataFrame

  def sum(): DataFrame

  def count(): DataFrame

  // TODO: Add var, std
}
