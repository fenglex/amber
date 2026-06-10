package ink.haifeng.spark

import org.apache.spark.sql.SparkSession

/**
 * SqlTest
 *
 * @author haifeng
 * @version 2022/12/7 8:51
 */
object SqlTest {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("test").master("local").getOrCreate()


    spark.createDataFrame(List(User2("name", 3), User2("name", 2), User2("name", 1), User2("name", 4), User2("name", 5), User2("name", 6))).createTempView("tb_test")

    val rows = spark.sql("SELECT sum(if(age>1,1,0)) from tb_test where age>10").collect()
    for (elem <- rows) {
      if (elem.isNullAt(0)) {
        println(1)
      } else {
        println(elem.getLong(0))
      }
    }
  }

}


case class User2(name: String, age: Int)
