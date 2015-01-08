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
import org.apache.spark.graphx.{Graph, TripletFields, VertexRDD}
import org.apache.spark.rdd.RDD

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

  object WarmStartOption extends Enumeration {
    type WarmStartOption = Value
    val WarmStartFromHubData, WarmStartFromAuthData, ColdStart = Value
  }

  import WarmStartOption._

  /**
   * Run HITS for a fixed number of iterations returning a graph
   * with vertex attributes containing the HIT (hub, auth) scores as a pair and
   * retains the original edge attributes
   *
   * @tparam VD the original vertex attribute (only used if starting warm)
   * @tparam ED the original edge attribute (returned)
   *
   * @param graph the graph on which to compute HITS
   * @param numIter the number of iterations of HITS to run
   * @param warmStartOption whether warm start data is provided
   *
   * @return the graph with each vertex containing the HITS (hub, scores) as a pair
   *         and each edge retaining its original value
   *
   */
  def run[VD: ClassTag, ED: ClassTag](
      graph: Graph[VD, ED],
      numIter: Int,
      warmStartOption: WarmStartOption = ColdStart): Graph[(Double, Double), ED] = {

    // Returns a graph with the given vertex data scaled down to have norm 1
    def setRescaled(updates: VertexRDD[Double]): Graph[Double, ED] = {
        updates.cache()
        // Use fold instead of sum, since sum calls reduce and reduce cannot handle empty lists
        val scale = Math.sqrt(updates.values.map(x => x * x).fold(0.0)(_ + _)) match {
          case 0.0 => 1.0
          case x => x
        }

        // Uses outerJoinVertices since missing data should be set to be 0.
        // Requires a shuffle for broadcasting updated scores to the edge partitions.
        val updatedGraph = graph.outerJoinVertices(updates) {
          (id, auth, msgSum) => msgSum.getOrElse(0.0)/scale
        }
        updates.unpersist(false)
        updatedGraph
    }

    def computeAuthFromHub(hubGraph: Graph[Double, ED],
        rescale: Boolean = true): Graph[Double, ED] = {
      // First compute the new auth values.
      // Requires a shuffle for aggregation.
      val authUpdates = hubGraph.aggregateMessages[Double](
        ctx => ctx.sendToDst(ctx.srcAttr), _ + _, TripletFields.Src)
      
      if (rescale) {
        setRescaled(authUpdates)
      }
      else {
        graph.outerJoinVertices(authUpdates) {
          (id, auth, msgSum) => msgSum.getOrElse(0.0)
        }
      }
    }

    def computeHubFromAuth(authGraph: Graph[Double, ED],
        rescale: Boolean = true): Graph[Double, ED] = {
      // First compute the new hub values.
      // Requires a shuffle for aggregation.
      val hubUpdates = authGraph.aggregateMessages[Double](
        ctx => ctx.sendToSrc(ctx.dstAttr), _ + _, TripletFields.Dst)
      
      if (rescale) {
        setRescaled(hubUpdates)
      }
      else {
        graph.outerJoinVertices(hubUpdates) {
          (id, auth, msgSum) => msgSum.getOrElse(0.0)
        }
      }
    }

    graph.cache()

    // If warm starting from auth data then we set the auth data
    //   and have to compute the new hub data before the first iteration
    // If warm starting from hub data then we just need to set the hub data

    var authGraph: Graph[Double, ED] = warmStartOption match {
      case WarmStartFromAuthData => graph.asInstanceOf[Graph[Double, ED]]
      case _ => graph.mapVertices((id, attr) => 1.0)
    }

    var hubGraph: Graph[Double, ED] = warmStartOption match {
      case WarmStartFromHubData => graph.asInstanceOf[Graph[Double, ED]]
      case WarmStartFromAuthData => computeHubFromAuth(authGraph)
      case _ => graph.mapVertices((id, attr) => 1.0)
    }

    // RDD[Double] is more efficient here, hence the seperate graphs
    var iteration = 0
    while (iteration < numIter - 1) {

      logInfo(s"HITS iteration $iteration.")

      // Rescaling only needs to be done in intermediate steps to prevent overflow.
      // Even with 10^12 vertices, in 5 iterations the values could go up only by a factor of 10^120
      //   which is less than the max double (10^308)
      val rescale: Boolean = (iteration%5 == 4)

      authGraph = computeAuthFromHub(hubGraph, rescale)
      hubGraph = computeHubFromAuth(authGraph, false)

      iteration += 1
    }

    // The last iteration is different because we want to rescale and we want to save the results.
    if (numIter >= 1) {
      authGraph = computeAuthFromHub(hubGraph).cache()
      hubGraph = computeHubFromAuth(authGraph).cache()
    }
    // Combine hub and auth into a single vertex attribute.
    hubGraph.outerJoinVertices(authGraph.vertices) { (id, hub, auth) => (hub, auth.get) }
  }

}
