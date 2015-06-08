package org.apache.flink.streaming.scala.examples.clustering

import org.apache.flink.streaming.api.scala._
import org.apache.flink.api.common.functions.RichMapFunction
import scala.util.Random
import java.util.Arrays
import java.util.ArrayList
import scala.collection.mutable.MutableList
import org.apache.flink.core.fs.FileSystem

object StreamClustering {

  case class Point(id: Int, x: Double, y: Double)

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val input = env.readTextFile("/Users/gyfora/OneDrive/LIU/streamclustering/data_small.csv").map(line => {
      val split = line.split(",")
      Point(split(0).toInt, split(1).toDouble, split(2).toDouble)
    })

    val clustered = input.map(new StreamKMeans(4))

    clustered.map(cp => (cp._1.id,cp._1.x,cp._1.y,cp._2)).writeAsCsv("/Users/gyfora/OneDrive/LIU/streamclustering/result_small.csv", writeMode = FileSystem.WriteMode.OVERWRITE)
//    clustered.print
    env.execute

  }

  def dist(p1: Point, p2: Point): Double = Math.sqrt((p2.x - p1.x) * (p2.x - p1.x) + (p2.y - p1.y) * (p2.y - p1.y))
  
  def nonBiasedCenter(currentCenter: (Point,Int), point: Point): (Point,Int) = {
    val n = currentCenter._2
    (Point(0,(currentCenter._1.x*n + point.x)/(n+1), (currentCenter._1.y*n + point.y)/(n+1)),n+1)
  }

  val dummy_clusterer = new RichMapFunction[Point, (Point, Int)] {
    def map(point: Point): (Point, Int) = {
      (point, Random.nextInt(5))
    }
  }

  class StreamKMeans(numClusters: Int, computeCenter: ((Point,Int),Point) => (Point,Int) = nonBiasedCenter) extends RichMapFunction[Point, (Point, Int)] {

    var centers = new MutableList[(Point,Int)]

    def map(point: Point): (Point, Int) = {
      if (centers.size < numClusters) { centers = centers :+ (point, centers.size); (point, centers.size - 1) } else
        (point, cluster(point))
    }

    def cluster(point: Point): Int = {

      var currentCenter = (Point(0, 0, 0), 0)
      var minDist: Double = 999
      var currentCluster = 0

      for (i <- 0 to numClusters - 1) {
        val center = centers(i)
        val d = dist(center._1, point)
        if (d < minDist) {
          currentCenter = center
          minDist = d
          currentCluster = i
        }
      }
      
      
      centers(currentCluster) = computeCenter(currentCenter,point)
      currentCluster
    }
  }

}