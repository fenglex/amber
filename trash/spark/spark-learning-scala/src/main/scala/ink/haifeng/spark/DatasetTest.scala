package ink.haifeng.spark

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}

/**
 * DatasetTest
 *
 * @author haifeng
 * @version 2022/12/23 10:18
 */
object DatasetTest {
  def main(args: Array[String]): Unit = {
    val session = SparkSession.builder().appName("test").master("local").getOrCreate()

    import session.implicits._
    val frame = List("hello spark",
      "hello spark",
      "hello spark",
      "hello spark",
      "hello spark",
      "hello spark",
      "hello spark").toDF("line")

    frame.createTempView("test")

    session.sql("select explode(split(line,' ')) as word from test").show()

    val structType = StructType.apply(Array(StructField.apply("score", IntegerType, nullable = false)))
    frame.selectExpr("explode(split(line,' ')) as word").groupBy("word").count().show()
  }

}
