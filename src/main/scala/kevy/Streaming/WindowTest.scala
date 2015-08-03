package kevy.Streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * Created by db-oracle on 15-7-31.
 */

object WindowTest {
  def main(args: Array[String]) {
    if(args.length <2){
      System.err.println("Usage: WindowTest <master> <hostname> <port> <seconds>")
    }
    val conf = new SparkConf()
    conf.setMaster(args(0))
    conf.setAppName("windowstest")
    val ssc = new StreamingContext(conf,Seconds(args(3).toInt))
    ssc.checkpoint(".")
    val lines = ssc.socketTextStream(args(1),args(2).toInt)
    val words = lines.flatMap(_.split(" "))
    val wordCounts = words.map((_,1)).reduceByKey(_ + _)
    wordCounts.print()
    ssc.start()
    ssc.awaitTermination()
  }
}
