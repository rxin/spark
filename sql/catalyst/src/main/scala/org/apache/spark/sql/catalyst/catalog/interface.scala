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

package org.apache.spark.sql.catalyst.catalog

import org.apache.spark.sql.AnalysisException


/**
 * Interface for the system catalog (of columns, partitions, tables, and databases).
 *
 * This is only used for non-temporary items, and implementations must be thread-safe as they
 * can be accessed in multiple threads.
 */
abstract class Catalog {

  // --------------------------------------------------------------------------
  // Databases
  // --------------------------------------------------------------------------

  @throws[AnalysisException]("if database already exists and ifNotExists is false")
  def createDatabase(dbDefinition: Database, ifNotExists: Boolean): Unit

  @throws[AnalysisException]("if database does not exist and ignoreIfNotExists is false")
  def dropDatabase(
    db: String,
    ignoreIfNotExists: Boolean,
    cascade: Boolean): Unit

  def alterDatabase(db: String, dbDefinition: Database): Unit

  def getDatabase(db: String): Database

  def listDatabases(): Seq[String]

  def listDatabases(pattern: String): Seq[String]

  // --------------------------------------------------------------------------
  // Tables
  // --------------------------------------------------------------------------

  def createTable(db: String, tableDefinition: Table, ifNotExists: Boolean): Unit

  def dropTable(db: String, table: String, ignoreIfNotExists: Boolean): Unit

  def renameTable(db: String, oldName: String, newName: String): Unit

  def alterTable(db: String, table: String, tableDefinition: Table): Unit

  def getTable(db: String, table: String): Table

  def listTables(db: String): Seq[String]

  def listTables(db: String, pattern: String): Seq[String]

  // --------------------------------------------------------------------------
  // Partitions
  // --------------------------------------------------------------------------

  // TODO: need more functions for partitioning.

  def alterPartition(db: String, table: String, part: TablePartition): Unit

  def alterPartitions(db: String, table: String, parts: Seq[TablePartition]): Unit

  // --------------------------------------------------------------------------
  // Functions
  // --------------------------------------------------------------------------

  def createFunction(db: String, funcDefinition: Function, ifNotExists: Boolean): Unit

  def dropFunction(db: String, funcName: String): Unit

  def alterFunction(db: String, funcName: String, funcDefinition: Function): Unit

  def getFunction(db: String, funcName: String): Function

  def listFunctions(db: String, pattern: String): Seq[String]

}


/**
 * A function defined in the catalog.
 *
 * @param name name of the function
 * @param className fully qualified class name, e.g. "org.apache.spark.util.MyFunc"
 */
case class Function(
  name: String,
  className: String
)


/**
 * Storage format, used to describe how a partition or a table is stored.
 */
case class StorageFormat(
  locationUri: String,
  inputFormat: String,
  outputFormat: String,
  serde: String,
  serdeProperties: Map[String, String]
)


/**
 * A column in a table.
 */
case class Column(
  name: String,
  dataType: String,
  nullable: Boolean,
  comment: String
)


/**
 * A partition (Hive style) defined in the catalog.
 *
 * @param values values for the partition columns
 * @param storage storage format of the partition
 */
case class TablePartition(
  values: Seq[String],
  storage: StorageFormat
)


/**
 * A table defined in the catalog.
 */
case class Table(
  name: String,
  schema: Seq[Column],
  partitionColumns: Seq[Column],
  sortColumns: Seq[Column],
  storage: StorageFormat,
  numBuckets: Int,
  properties: Map[String, String],
  tableType: String,
  createTime: Long,
  lastAccessTime: Long,
  viewOriginalText: Option[String],
  viewText: Option[String]) {

  require(tableType == "EXTERNAL_TABLE" || tableType == "INDEX_TABLE" ||
    tableType == "MANAGED_TABLE" || tableType == "VIRTUAL_VIEW")
}


/**
 * A database defined in the catalog.
 */
case class Database(
  name: String,
  description: String,
  locationUri: String,
  properties: Map[String, String]
)
