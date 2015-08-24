package com.soteradefense.graphbenchmarks

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.SparkContext._
import org.apache.spark._
import org.apache.spark.rdd.RDD


/**
 * Clean an edge list data to ensure the following:
 * 1. edges are un-directed
 * 2. no duplicate edges
 * 3. no self edges.
 *
 * This gives provides some simple rules for input datasets to help
 * ensure fair benchmarking across platforms.
 *
 * args inputFile numberOfPartitions outputPath
 *
 */

object CleanData {

  val appName = "clean-data"
  val DELIMITER = ","

  def main(args: Array[String]) {

    if (args.length != 3){
      println("filePath , numPartitions, and outputPath are required command line arguments")
      System.exit(1)
    }

    val conf = new SparkConf().setAppName(appName)
    val sc = new SparkContext(conf)

    val t1 = System.currentTimeMillis
    val FILE = args(0)
    val PARTITIONS = args(1).toInt
    val OUT_FILE = args(2)
    println(s"inputPath: $FILE partitions: $PARTITIONS outputPath: $OUT_FILE")

    val uniqueEdges = sc.textFile(FILE,PARTITIONS)
    .map(x=> {
        val edge = x.trim.split(DELIMITER)
        val src = edge(0).toInt
        val dst = edge(1).toInt
        if (src < dst) (src,dst) else (dst,src)
    }).distinct()
    .map({case (src,dst) => src+","+dst})
    .repartition(PARTITIONS)
    .saveAsTextFile(OUT_FILE)


  }


}
