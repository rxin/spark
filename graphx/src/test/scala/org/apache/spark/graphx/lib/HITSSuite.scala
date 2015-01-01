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

import org.scalatest.FunSuite

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.graphx._
import org.apache.spark.graphx.lib._
import org.apache.spark.graphx.util.GraphGenerators
import org.apache.spark.rdd._

// Helper class that computes HITS for a Grid Graph
object GridHITS {

  def apply(nRows: Int, nCols: Int, nIter: Int) = {
    val inNbrs = Array.fill(nRows * nCols)(collection.mutable.MutableList.empty[Int])
    val outNbrs = Array.fill(nRows * nCols)(collection.mutable.MutableList.empty[Int])
    // Convert row column address into vertex ids (row major order)
    def sub2ind(r: Int, c: Int): Int = r * nCols + c
    // scale an array to have length 1
    def normalize(arr: Array[Double]): Array[Double] = {
      val normArr = Math.sqrt(arr.map(x => x*x).sum)
      arr.map(x => x / normArr)
    }
    // Make the grid graph
    for (r <- 0 until nRows; c <- 0 until nCols) {
      val ind = sub2ind(r,c)
      if (r+1 < nRows) {
        inNbrs(sub2ind(r+1,c)) += ind
        outNbrs(ind) += sub2ind(r+1,c)
      }
      if (c+1 < nCols) {
        inNbrs(sub2ind(r,c+1)) += ind
        outNbrs(ind) += sub2ind(r,c+1)
      }
    }
    // compute the hub and auth scores
    var hub = Array.fill(nRows * nCols)(1.0)
    var auth = Array.fill(nRows * nCols)(1.0)
    for (iter <- 0 until nIter) {
      for (ind <- 0 until (nRows * nCols))
        auth(ind) = inNbrs(ind).map( nbr => hub(nbr)).sum
      auth = normalize(auth)
      for (ind <- 0 until (nRows * nCols))
        hub(ind) = outNbrs(ind).map( nbr => auth(nbr)).sum
      hub = normalize(hub)
    }
    (0L until (nRows * nCols)).zip(hub.zip(auth))
  }

}


class HITSSuite extends FunSuite with LocalSparkContext {

  def compareHits(a: VertexRDD[(Double,Double)], b: VertexRDD[(Double,Double)]): Double = {
    a.leftJoin(b){ (id, a, b) => Math.abs(a._1 - b.get._1) + Math.abs(a._2 - b.get._2) }.values.sum
  }

  // Compare result on Star Shaped graph with the theoretical value
  test("Star HITS") {
    withSpark { sc =>
      val starGraph = GraphGenerators.starGraph(sc, 257).cache()
      val errorTol = 1.0e-5

      val hits1 = starGraph.staticHITS(numIter = 1).vertices.cache()
      val hits2 = starGraph.staticHITS(numIter = 2).vertices.cache()

      val referenceHits = starGraph.mapVertices ( (id,attr) =>
          if (id == 0) (0.0, 1.0)
          else (0.0625, 0.0)
      ).vertices.cache()

      assert(compareHits(referenceHits, hits1) < errorTol)
      assert(compareHits(hits1, hits2) < errorTol)

    }
  } // end of test Star HITS


  // Compare result on Grid graph with the values computed by GridHITS
  test("Grid HITS") {
    withSpark { sc =>
      val rows = 10
      val cols = 10
      val numIter = 25
      val errorTol = 1.0e-5
      val gridGraph = GraphGenerators.gridGraph(sc, rows, cols)

      val staticHits = gridGraph.staticHITS(numIter).vertices

      val referenceHits = VertexRDD(sc.parallelize(GridHITS(rows, cols, numIter)))

      assert(compareHits(referenceHits, staticHits) < errorTol)
    }
  } // end of Grid HITS


  // Compare result on Chain graph (10 vertices connected in a row) with the theoretical value
  test("Chain HITS") {
    withSpark { sc =>
      val rawEdges = sc.parallelize((0L until 9L).map(x => (x, x+1) ))
      val chain = Graph.fromEdgeTuples(rawEdges, 1.0).cache()
      val numIter = 10
      val errorTol = 1.0e-5

      val staticHits = chain.staticHITS(numIter).vertices

      val referenceHits = chain.mapVertices ( (id,attr) => id match {
          case 0 => (1/3.0, 0.0)
          case 9 => (0.0, 1/3.0)
          case _ => (1/3.0, 1/3.0)
      }).vertices

      assert(compareHits(referenceHits, staticHits) < errorTol)
    }
  } // end of Chain HITS


  // Make sure that the result on an empty graph is all zero
  test("Empty HITS") {
    withSpark { sc =>
      val graph = GraphGenerators.starGraph(sc, 10).subgraph(e => false).cache
      val numIter = 25
      val errorTol = 1.0e-5

      val staticHits = graph.staticHITS(numIter).vertices.cache

      val referenceHits = graph.mapVertices ( (id,attr) => (0.0, 0.0) ).vertices

      assert(compareHits(referenceHits, staticHits) < errorTol)
    }
  } // end of Empty HITS


  // Compare result on a graph composed of multiple cycles and a chain, with the theoretical value
  test("Cycle Forest HITS") {
    withSpark { sc =>
      val rawEdges = sc.parallelize((0L until 64).map(x => (x, x/10*10 + (x+1)%10) ))
      val graph = Graph.fromEdgeTuples(rawEdges, 1.0).cache()
      val numIter = 15
      val errorTol = 1.0e-5

      val staticHits = graph.staticHITS(numIter).vertices.cache

      val referenceHits = graph.mapVertices ( (id,attr) => id match {
          case 60 => (0.125, 0.0)
          case 64 => (0.0, 0.125) 
          case _ => (0.125, 0.125)
      }).vertices

      assert(compareHits(referenceHits, staticHits) < errorTol)
    }
  } // end of Cycle Forest HITS


}
