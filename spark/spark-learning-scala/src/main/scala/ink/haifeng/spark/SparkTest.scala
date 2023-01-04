package ink.haifeng.spark

import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

object SparkTest {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setAppName("test")
    conf.setMaster("local[2]")
    val sc = new SparkContext(conf)
    val accumulator = sc.longAccumulator("test")
    val rdd = sc.makeRDD(List(("a", 3), ("a", 2), ("a", 1), ("a", 4), ("a", 5), ("a", 1), ("b", 2), ("a", 2), ("d", 2)))
    // accumulator.

    rdd.foreach(println)

//    rdd.groupByKey().sortBy().sortBy(k => {
//      k._1 + k._2
//    }).groupBy(_._1).foreachPartition(iter => {
//      while (iter.hasNext) {
//        val tuple = iter.next()
//        val key = tuple._1
//        val iterator = tuple._2.iterator
//        while (iterator.hasNext) {
//          println(iter + "\t" + key + " -->" + iterator.next())
//        }
//      }
//    })
    //    rdd.flatMap(_.split(" ")).map((_, 1)).sortBy(_._1).groupBy((k, v) -> {
    //
    //    })

    sc.stop()
  }

}
