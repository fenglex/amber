package ink.haifeng.spark

import org.apache.spark.{SparkConf, SparkContext}

object SparkTest {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setAppName("test")
    conf.setMaster("local[2]")
    val sc = new SparkContext(conf)
    val rdd = sc.makeRDD(List("a", "b", "a", "d"))
    rdd.flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _).collect().foreach(println(_))

    sc.stop()
  }

}
