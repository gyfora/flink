package org.apache.flink.streaming.scala.examples.clustering

import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.scala.examples.clustering.StreamClustering._
import org.apache.flink.api.common.functions.RichMapFunction
import scala.util.Random
import java.util.Arrays
import java.util.ArrayList
import scala.collection.mutable.MutableList
import org.apache.flink.core.fs.FileSystem
import org.apache.flink.api.common.functions.MapFunction

object SimpleStreamClustering {

  case class Point(id: Int, x: Double, y: Double)

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val input = readInput("/Users/gyfora/OneDrive/LIU/streamclustering/data_small.csv", env)

//    clusterNonParallel(input, new SimpleKMeans(4), "/Users/gyfora/OneDrive/LIU/streamclustering/nbKmeans.csv")
//    clusterNonParallel(input, new SimpleKMeans(4, timeBiasedMean(0.1)), "/Users/gyfora/OneDrive/LIU/streamclustering/tbKmeans.csv")
//    clusterNonParallel(input, new SimpleKMeans(4, timeBiasedMean(0.8)), "/Users/gyfora/OneDrive/LIU/streamclustering/tbKmeans2.csv")
    
    clusterParallel(input, new SimpleKMeans(4), "/Users/gyfora/OneDrive/LIU/streamclustering/parallelNBKmeans.csv")

    println(env.getExecutionPlan);
    env.execute

  }

  
}