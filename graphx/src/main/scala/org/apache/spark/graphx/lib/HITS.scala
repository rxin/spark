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

package org.apache.spark.graphx.lib

import scala.reflect.ClassTag

import org.apache.spark.Logging
import org.apache.spark.graphx._
import org.apache.spark.rdd._

/**
 * HITS algorithm implementation.
 *
 * The implementation runs HITS for a fixed number of iterations:
 * {{{
 * var auth = Array.fill(n)( 1.0 )
 * var hub = Array.fill(n)( 1.0 )
 * for( iter <- 0 until numIter ) {
 *   for( i <- 0 until n ) {
 *     auth[i] = inNbrs[i].map(j => hub[j]).sum
 *   }
 *   
 *   for( i <- 0 until n ) {
 *     hub[i] = outNbrs[i].map(j => auth[j]).sum
 *   }
 *   // normalize auth and hub to norm 1
 * }
 * }}}
 *
 * `inNbrs[i]` is the set of neighbors whick link to `i` and
 * `outNbrs[j]` is the set of neighbouts which `i` links to
 *
 */
object HITS extends Logging {

  /**
   * Run HITS for a fixed number of iterations returning a graph
   * with vertex attributes containing the HIT (hub, auth) scores as a pair and
   * retains the original edge attributes
   *
   * @tparam VD the original vertex attribute (not used)
   * @tparam ED the original edge attribute (returned)
   *
   * @param graph the graph on which to compute HITS
   * @param numIter the number of iterations of HITS to run
   *
   * @return the graph with each vertex containing the HITS (hub, scores) as a pair
   *         and each edge retaining its original value
   *
   */
  def run[VD: ClassTag, ED: ClassTag](
      graph: Graph[VD, ED], numIter: Int): Graph[(Double, Double), ED] =
  {

    // Scale down non-zero vectors to have norm 1
    def scalingFactor(v: VertexRDD[Double]): Double = {
      // Use fold instead of sum, since sum calls reduce and reduce cannot handle empty lists
      val norm = Math.sqrt(v.values.map(x => x*x ).fold(0.0)(_ + _))
      if(norm == 0.0) 1.0
      else norm
    }

    // Initialize the HITS graph with each edge attribute having
    // its original attribute and each vertex with attribute 1.0
    var hubGraph = graph.mapVertices( (id, attr) => 1.0 ).cache
    var authGraph = graph.mapVertices( (id, attr) => 1.0 ).cache

    // RDD[Double] is more efficient here, hence the seperate graphs
    // Technically there is no need to rescale at every single iteration,
    //    but the cost of doing so is very small.
    var iteration = 0
    while (iteration < numIter) {

      // First compute the new auth values.
      // Requires a shuffle for aggregation.
      val authUpdates = hubGraph.aggregateMessages[Double](
        ctx => ctx.sendToDst(ctx.srcAttr), _ + _, TripletFields.Src).cache
      val authScale = scalingFactor(authUpdates)

      // Forget the old auth values, update to the new values, and unpersist the update RDD.
      // Uses outerJoinVertices since missing data should be set to be 0.
      // Requires a shuffle for broadcasting updated ranks to the edge partitions.
      authGraph.unpersistVertices(false)
      authGraph = graph.outerJoinVertices(authUpdates) {
        (id, auth, msgSum) => msgSum.getOrElse(0.0)/authScale
      }.cache()
      authGraph.edges.foreachPartition(x => {}) // also materializes hitsGraph.vertices
      authUpdates.unpersist(false)

      logInfo(s"HITS finished first half of iteration $iteration.")

      // Next compute the new hub values
      val hubUpdates = authGraph.aggregateMessages[Double](
        ctx => ctx.sendToSrc(ctx.dstAttr), _ + _, TripletFields.Dst).cache
      val hubScale = scalingFactor(hubUpdates)

      // Forget the old hub values, update to the new values, and unpersist the update RDD
      hubGraph.unpersistVertices(false)
      hubGraph = graph.outerJoinVertices(hubUpdates) {
        (id, hub, msgSum) => msgSum.getOrElse(0.0)/hubScale
      }.cache()
      hubGraph.edges.foreachPartition(x => {}) // also materializes hitsGraph.vertices
      hubUpdates.unpersist(false)

      logInfo(s"HITS finished second half of iteration $iteration.")

      iteration += 1
    }

    // Combine hub and auth into a single vertex attribute and unpersist hubGraph and authGraph
    val returnGraph = hubGraph
      .outerJoinVertices(authGraph.vertices) { (id, hub, auth) => (hub, auth.get) }
    hubGraph.unpersistVertices(false)
    authGraph.unpersistVertices(false)
    returnGraph
  }

}
