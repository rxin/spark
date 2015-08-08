package org.apache.spark.sql

import org.apache.spark.sql.test.SQLTestUtils

import org.apache.spark.sql.functions._


class DataFramePlanSuite extends QueryTest with SQLTestUtils {

  override lazy val sqlContext: SQLContext = org.apache.spark.sql.test.TestSQLContext
  import sqlContext.implicits._

  test("test simple types") {
    val df = sqlContext.read.parquet("/scratch/rxin/spark/sales4")
    val grouped = df.groupBy(df("date"), df("cat")).agg(sum(df("count") + 1))
      .sample(true, 0.1)
    grouped.explain(true)
  }

  test("test local execution") {
    val df = Seq((1, "a"), (2, "b")).toDF("a", "b")
    println(df.filter($"a" > 1).collectLocal())
  }
}
