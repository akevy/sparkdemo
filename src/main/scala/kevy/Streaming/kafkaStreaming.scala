package kevy.Streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * Created by db-oracle on 15-7-31.
 */
class kafkaStreaming {

}

object kafkaWordCount {
  def main(args: Array[String]) {
    if (args.length < 4) {
      System.err.println("Usage: KafkaWordCount <zkQuorum> <group> <topics> <numThreads>")
      System.exit(1)
    }
    val sparkConf = new SparkConf().setAppName("KafkaWordCount")
    val ssc = new StreamingContext(sparkConf, Seconds(2))
    ssc.checkpoint(".")
    val topicMap = scala.collection.immutable.Map(args(2) ->1)
    val lines = KafkaUtils.createStream(ssc,args(0),args(1),topicMap)
    val statDStream = lines.map(_._2).flatMap(_.split(" ")).map((_,1L)).reduceByKeyAndWindow(_+_,_-_, Seconds(5),Seconds(10),args(3).toInt)
    statDStream.print()
    ssc.start()
    ssc.awaitTermination()
  }
}

