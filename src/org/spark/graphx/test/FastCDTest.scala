package org.spark.graphx.test
import org.spark.graphx.FastCD
import org.apache.spark.SparkContext
import org.apache.spark._
import org.apache.spark.graphx._
import java.util.Date
object FastCDTest {
  def main(args:Array[String])={
       if(args.length<3){
          println("Param:<edgesFile> <verticeFile> <patition>")
          System.exit(0)
        }
       val edgesFile=args(0)
        val verticeFile=args(1)
        val numPartitions=args(2).toInt
        val starttime=new Date().getTime
        val sf = new SparkConf().setAppName("Spark Graphx FastCD")
        val sc = new SparkContext(sf)
        val graph=FastCD.run(sc,edgesFile, numPartitions, verticeFile)
       val endtime=new Date().getTime
       val ss=(endtime-starttime)/(1000)
       println("Runtime "+ss+"s")

        val data=FastCD.getQAndCommunityNumbers(sc,graph)
        println("Q => "+data._1+"\nThe number of community is "+data._2)
       sc.stop()
    
  }
}