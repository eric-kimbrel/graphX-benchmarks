package com.soteradefense.scanstat

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.SparkContext._
import org.apache.spark._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD


/**
 * Finds the vertex with the maximum neighborhood size.
 * (number of edges in the subgraph made up of a node and its neighbors)
 *
 * args: inputFile numberOfPartitions
 */


object ScanStat {

  val appName = "scanstat"

  def main(args: Array[String]) {

    if (args.length != 2){
      println("filePath and numPartitions are required command line arguments")
      System.exit(1)
    }

    val conf = new SparkConf().setAppName(appName)
    val sc = new SparkContext(conf)

    val t1 = System.currentTimeMillis
    val FILE = args(0)
    val PARTITIONS = args(1).toInt
    println(s"inputPath: $FILE partitions: $PARTITIONS")

    // read in edges from a csv file
    val edgesRDD = sc.textFile(FILE,PARTITIONS).map(edgeStr=> {
      val edgeArray = edgeStr.split(",")
      Edge(edgeArray(0).toInt,edgeArray(1).toInt,None)
    })

    //build the graph
    val graph = Graph.fromEdges(edgesRDD,None).partitionBy(org.apache.spark.graphx.PartitionStrategy.EdgePartition1D).cache()
    val nodes = graph.vertices.count
    val edges = graph.edges.count
    println(s"Nodes: $nodes Edges: $edges")
    val t2 = System.currentTimeMillis
    val readInTime = (t2-t1) / 1000.0


    val t3 = System.currentTimeMillis

    val adjList = graph.collectNeighborIds(EdgeDirection.Either)

    val adjGraph = graph.outerJoinVertices(adjList){ (id,oldAttr,adjOpt) =>
      adjOpt match{
        case Some(adj) => adj.map(_.toInt)
        case None => Array[Int]()
      }
    }

    // generate a new graph with the adjaceny list for each vertex as vertex data
    val messages = adjGraph.aggregateMessages[(Int,Int)](
      triplet => {
        val srcNeighbors = triplet.srcAttr.toSet
        val edgesInNeighborhood = triplet.dstAttr.filter(x => srcNeighbors(x) ).length
        triplet.sendToSrc((1,edgesInNeighborhood))
        triplet.sendToDst((1,edgesInNeighborhood))
      },
      (a,b) => (a._1+b._1,a._2+b._2)
    )

    val neighborhoodSize = messages.map({case (id,(srcEdges,neighboringEdges)) => (id,srcEdges+(neighboringEdges)/2)})
    val maxScan = neighborhoodSize.reduce({case ((id1,scan1),(id2,scan2) ) => if (scan1 > scan2) (id1,scan1) else (id2,scan2) } )

    val t4 = System.currentTimeMillis
    val scanTime = (t4-t3) / 1000.0

    println(s"maxScan: $maxScan\nGraph Read in and Cache-count time: $readInTime\nScan Time: $scanTime seconds.")




  }


}


