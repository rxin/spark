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

import org.apache.spark.sql.test.SharedSQLContext

class QueryFragmentSuite extends QueryTest with SharedSQLContext {

  import testImplicits._

  test("query plan") {
    val df = Seq((1, "one"), (2, "two")).toDF("number", "alphabet")

    df.filter($"number" > 3).select($"number" + 1).explain(true)
  }

  test("read local") {
    val df = Seq((1, "one"), (2, "two"), (3, "three")).toDF("number", "alphabet")

    println(df.filter($"number" > 1).select($"number" + 1).collect().toSeq)
  }
}
