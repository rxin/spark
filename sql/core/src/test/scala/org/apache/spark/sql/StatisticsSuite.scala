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

import java.{lang => jl}
import java.sql.{Date, Timestamp}

import scala.collection.mutable

import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.parser.ParseException
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.execution.command.AnalyzeColumnCommand
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.test.SharedSQLContext
import org.apache.spark.sql.test.SQLTestData.ArrayData
import org.apache.spark.sql.types._

/**
 * End-to-end suite testing statistics collection and use
 * on both entire table and individual columns.
 */
class StatisticsSuite extends QueryTest with SharedSQLContext {
  import testImplicits._

  private def checkTableStats(tableName: String, expectedRowCount: Option[Int])
    : Option[Statistics] = {
    val df = spark.table(tableName)
    val stats = df.queryExecution.analyzed.collect { case rel: LogicalRelation =>
      assert(rel.catalogTable.get.stats.flatMap(_.rowCount) === expectedRowCount)
      rel.catalogTable.get.stats
    }
    assert(stats.size == 1)
    stats.head
  }

  test("SPARK-15392: DataFrame created from RDD should not be broadcasted") {
    val rdd = sparkContext.range(1, 100).map(i => Row(i, i))
    val df = spark.createDataFrame(rdd, new StructType().add("a", LongType).add("b", LongType))
    assert(df.queryExecution.analyzed.statistics.sizeInBytes >
      spark.sessionState.conf.autoBroadcastJoinThreshold)
    assert(df.selectExpr("a").queryExecution.analyzed.statistics.sizeInBytes >
      spark.sessionState.conf.autoBroadcastJoinThreshold)
  }

  test("estimates the size of limit") {
    withTempView("test") {
      Seq(("one", 1), ("two", 2), ("three", 3), ("four", 4)).toDF("k", "v")
        .createOrReplaceTempView("test")
      Seq((0, 1), (1, 24), (2, 48)).foreach { case (limit, expected) =>
        val df = sql(s"""SELECT * FROM test limit $limit""")

        val sizesGlobalLimit = df.queryExecution.analyzed.collect { case g: GlobalLimit =>
          g.statistics.sizeInBytes
        }
        assert(sizesGlobalLimit.size === 1, s"Size wrong for:\n ${df.queryExecution}")
        assert(sizesGlobalLimit.head === BigInt(expected),
          s"expected exact size $expected for table 'test', got: ${sizesGlobalLimit.head}")

        val sizesLocalLimit = df.queryExecution.analyzed.collect { case l: LocalLimit =>
          l.statistics.sizeInBytes
        }
        assert(sizesLocalLimit.size === 1, s"Size wrong for:\n ${df.queryExecution}")
        assert(sizesLocalLimit.head === BigInt(expected),
          s"expected exact size $expected for table 'test', got: ${sizesLocalLimit.head}")
      }
    }
  }

  test("estimates the size of a limit 0 on outer join") {
    withTempView("test") {
      Seq(("one", 1), ("two", 2), ("three", 3), ("four", 4)).toDF("k", "v")
        .createOrReplaceTempView("test")
      val df1 = spark.table("test")
      val df2 = spark.table("test").limit(0)
      val df = df1.join(df2, Seq("k"), "left")

      val sizes = df.queryExecution.analyzed.collect { case g: Join =>
        g.statistics.sizeInBytes
      }

      assert(sizes.size === 1, s"number of Join nodes is wrong:\n ${df.queryExecution}")
      assert(sizes.head === BigInt(96),
        s"expected exact size 96 for table 'test', got: ${sizes.head}")
    }
  }

  test("test table-level statistics for data source table created in InMemoryCatalog") {
    val tableName = "tbl"
    withTable(tableName) {
      sql(s"CREATE TABLE $tableName(i INT, j STRING) USING parquet")
      Seq(1 -> "a", 2 -> "b").toDF("i", "j").write.mode("overwrite").insertInto(tableName)

      // noscan won't count the number of rows
      sql(s"ANALYZE TABLE $tableName COMPUTE STATISTICS noscan")
      checkTableStats(tableName, expectedRowCount = None)

      // without noscan, we count the number of rows
      sql(s"ANALYZE TABLE $tableName COMPUTE STATISTICS")
      checkTableStats(tableName, expectedRowCount = Some(2))
    }
  }

  test("analyze column command - parsing") {
    val tableName = "column_stats_test0"

    // we need to specify column names
    intercept[ParseException] {
      sql(s"ANALYZE TABLE $tableName COMPUTE STATISTICS FOR COLUMNS")
    }

    val analyzeSql = s"ANALYZE TABLE $tableName COMPUTE STATISTICS FOR COLUMNS key, value"
    val parsed = spark.sessionState.sqlParser.parsePlan(analyzeSql)
    val expected = AnalyzeColumnCommand(TableIdentifier(tableName), Seq("key", "value"))
    comparePlans(parsed, expected)
  }

  test("analyze column command - unsupported types and invalid columns") {
    val tableName = "column_stats_test1"
    withTable(tableName) {
      Seq(ArrayData(Seq(1, 2, 3), Seq(Seq(1, 2, 3)))).toDF().write.saveAsTable(tableName)

      // Test unsupported data types
      val err1 = intercept[AnalysisException] {
        sql(s"ANALYZE TABLE $tableName COMPUTE STATISTICS FOR COLUMNS data")
      }
      assert(err1.message.contains("does not support statistics collection"))

      // Test invalid columns
      val err2 = intercept[AnalysisException] {
        sql(s"ANALYZE TABLE $tableName COMPUTE STATISTICS FOR COLUMNS some_random_column")
      }
      assert(err2.message.contains("does not exist"))
    }
  }

  private val dec1 = new java.math.BigDecimal("1.000000000000000000")
  private val dec2 = new java.math.BigDecimal("8.000000000000000000")
  private val d1 = Date.valueOf("2016-05-08")
  private val d2 = Date.valueOf("2016-05-09")
  private val t1 = Timestamp.valueOf("2016-05-08 00:00:01")
  private val t2 = Timestamp.valueOf("2016-05-09 00:00:02")

  /**
   * Define a very simple 3 row table used for testing column serialization.
   * Note: last column is seq[int] which doesn't support stats collection.
   */
  private val data = Seq[
    (jl.Boolean, jl.Byte, jl.Short, jl.Integer, jl.Long,
      jl.Double, jl.Float, java.math.BigDecimal,
      String, Array[Byte], Date, Timestamp,
      Seq[Int])](
    (false, 1.toByte, 1.toShort, 1, 1L, 1.0, 1.0f, dec1, "s1", "b1".getBytes, d1, t1, null),
    (true, 2.toByte, 3.toShort, 4, 5L, 6.0, 7.0f, dec2, "ss9", "bb0".getBytes, d2, t2, null),
    (null, null, null, null, null, null, null, null, null, null, null, null, null)
  )

  /** A mapping from column to the stats collected. */
  private val stats = mutable.LinkedHashMap(
    "cbool" -> ColumnStat(2, Some(false), Some(true), 1, 1, 1),
    "cbyte" -> ColumnStat(2, Some(1L), Some(2L), 1, 1, 1),
    "cshort" -> ColumnStat(2, Some(1L), Some(3L), 1, 2, 2),
    "cint" -> ColumnStat(2, Some(1L), Some(4L), 1, 4, 4),
    "clong" -> ColumnStat(2, Some(1L), Some(5L), 1, 8, 8),
    "cdouble" -> ColumnStat(2, Some(1.0), Some(6.0), 1, 8, 8),
    "cfloat" -> ColumnStat(2, Some(1.0), Some(7.0), 1, 4, 4),
    "cdecimal" -> ColumnStat(2, Some(dec1), Some(dec2), 1, 16, 16),
    "cstring" -> ColumnStat(2, None, None, 1, 3, 3),
    "cbinary" -> ColumnStat(2, None, None, 1, 3, 3),
    "cdate" -> ColumnStat(2, Some(d1), Some(d2), 1, 4, 4),
    "ctimestamp" -> ColumnStat(2, Some(t1), Some(t2), 1, 8, 8)
  )

  test("column stats round trip serialization") {
    // Make sure we serialize and then deserialize and we will get the result data
    val df = data.toDF(stats.keys.toSeq :+ "carray" : _*)
    stats.zip(df.schema).foreach { case ((k, v), field) =>
      withClue(s"column $k with type ${field.dataType}") {
        val roundtrip = ColumnStat.fromMap("table_is_foo", field, v.toMap)
        assert(roundtrip == Some(v))
      }
    }
  }

  test("analyze column command - result verification") {
    val tableName = "column_stats_test2"
    // (data.head.productArity - 1) because the last column does not support stats collection.
    assert(stats.size == data.head.productArity - 1)
    val df = data.toDF(stats.keys.toSeq :+ "carray" : _*)

    withTable(tableName) {
      df.write.saveAsTable(tableName)

      // Collect statistics
      sql(s"analyze table $tableName compute STATISTICS FOR COLUMNS " + stats.keys.mkString(", "))

      // Validate statistics
      val table = spark.sessionState.catalog.getTableMetadata(TableIdentifier(tableName))
      assert(table.stats.isDefined)
      assert(table.stats.get.colStats.size == stats.size)

      stats.foreach { case (k, v) =>
        withClue(s"column $k") {
          assert(table.stats.get.colStats(k) == v)
        }
      }
    }
  }
}
