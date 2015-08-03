package kevy.Streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * Created by db-oracle on 15-8-2.
 */

object KafkaCountSearch{
  def  main (args: Array[String]) {
    if (args.length < 3) {
      System.err.println("Usage: KafkaWordCount <zkQuorum> <group> <topics> ")
      System.exit(1)
    }

    val sparkConf = new SparkConf().setAppName("Kafka UpdateCount")
    val ssc = new StreamingContext(sparkConf, Seconds(5))
    ssc.checkpoint(".")
    val topicMap = scala.collection.immutable.Map(args(2) ->1)
    val lines = KafkaUtils.createStream(ssc,args(0),args(1),topicMap)
    val kafkaDStream = lines.map(_._2).map(_.split("\t")(3)).map((_,1))
    val stateDStream = kafkaDStream.updateStateByKey[Int](updateFunc)
    stateDStream.saveAsTextFiles("hdfs:///user/spark/SougoWrod")
    ssc.start()
    ssc.awaitTermination()

  }

  val updateFunc = (values : Seq[Int],state :Option[Int]) =>{
    val currentCount = values.foldLeft(0)(_ + _)
    val previousCount = state.getOrElse(0)
    Some(currentCount + previousCount)
  }
}