package com.soteradefense.scanstat

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.SparkContext._
import org.apache.spark._
import org.apache.spark.graphx._


/**
 * Calculates the page rank for each node of the graph
 * convergence tolerance is 0.001
 * damping factor is 0.85
 *
 * args:  inputFile numberOfPartitions outputFile
 */

object PageRank {

  val appName = "pagerank"

  def main(args: Array[String]) {

    if (args.length != 3){
      println("filePath , numPartitions, and output path are required command line arguments")
      System.exit(1)
    }

    val conf = new SparkConf().setAppName(appName)
    val sc = new SparkContext(conf)

    val t1 = System.currentTimeMillis
    val FILE = args(0)
    val PARTITIONS = args(1).toInt
    val OUTFILE = args(2)
    println(s"inputPath: $FILE partitions: $PARTITIONS outputPath: $OUTFILE")

    // read in edges from a csv file
    // copy edges to both directions.
    val edgesRDD = sc.textFile(FILE,PARTITIONS).flatMap(edgeStr=> {
      val edgeArray = edgeStr.split(",")
      Seq(Edge(edgeArray(0).toInt,edgeArray(1).toInt,None),Edge(edgeArray(1).toInt,edgeArray(0).toInt,None))
    })

    //build the graph
    val graph = Graph.fromEdges(edgesRDD,None).partitionBy(org.apache.spark.graphx.PartitionStrategy.EdgePartition1D).cache()
    val nodes = graph.vertices.count
    val edges = graph.edges.count
    println(s"Nodes: $nodes Edges: $edges")
    val t2 = System.currentTimeMillis



    val t3 = System.currentTimeMillis
    val ranks =  graph.pageRank(0.001,0.15).vertices
    ranks.cache()
    ranks.count()
    val t4 = System.currentTimeMillis

    ranks.map({case (id,value) => s"$id,$value" }).saveAsTextFile(OUTFILE)
    val t5 = System.currentTimeMillis


    val readInTime = (t2-t1) / 1000.0
    val writeTime = (t5-t4) / 1000.0
    val pagerankTime = (t4 - t3)  /1000.01
    println(s"\nread: $readInTime\n write: $writeTime\n pagerank: $pagerankTime\n")





  }


}


