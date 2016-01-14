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

import org.scalatest.BeforeAndAfter

import org.apache.spark.sql.test.SharedSQLContext


class LocalPlanSuite extends QueryTest with BeforeAndAfter with SharedSQLContext {

  import testImplicits._

  test("plan test") {

    sqlContext.range(100).select(('id + 1).as('id)).filter('id > 10).explain(true)

    sqlContext.range(100).select(('id + 1).as('id)).repartition(10).filter('id > 10).explain(true)

    sqlContext.range(100).select(('id + 5).as('id)).filter('id > 90).show(100)
  }

}
