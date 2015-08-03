package kevy

import org.apache.spark.{SparkConf, SparkContext}
//import org.spark.SparkContext._



/**
 * Created by chenchao on 14-3-1.
 */
class Analysis {

}

object Analysis{

  def main(args : Array[String]){

//    检测参数是否符合规范
    if(args.length != 2){
      println("Usage : java -jar code.jar dependency_jars file_location save_location")
      System.exit(0)
    }

//    获取参数列表
//    val jars = ListBuffer[String]()
//    args(0).split(',').map(jars += _)

//    类似hadoop的configuration，获取和设置配置文件
    val conf = new SparkConf()
      .setAppName("analysis")
//      .setJars(jars)
      .set("spark.executor.memory","256m")

//    获取sc
    val sc = new SparkContext(conf)
    val data = sc.textFile(args(0))

    data.cache

    println(data.count)

    data.filter(_.split(' ').length == 3).map(_.split(' ')(1)).map((_,1)).reduceByKey(_+_).saveAsTextFile(args(1))
//      .map(x => (x._2, x._1)).sortByKey(false).map( x => (x._2, x._1)).saveAsTextFile(args(2))
  }

}
