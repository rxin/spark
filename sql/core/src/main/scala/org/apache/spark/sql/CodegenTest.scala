package org.apache.spark.sql

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.functions._

object CodegenTest {

  def main(args: Array[String]): Unit = {
    val sc = SparkContext.getOrCreate()
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._

    val n = args(0).toInt
    for (i <- 1 to n) {
      val start = System.currentTimeMillis()
      val df = sc.parallelize(1 to 1000000).map(i => (i, i.toString)).toDF("a", "b")
      // use "+ i" to avoid reusing the same generated code.
      df.select(Seq.fill(100)(sum(df("a") + i)) : _*).collect()
      val timeTaken = System.currentTimeMillis() - start
      println(s"iter $i time taken: $timeTaken")
    }
  }
}
