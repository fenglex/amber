package ink.haifeng.spark

import org.apache.spark.sql.SparkSession

/**
 * LineToRow
 *
 * @author haifeng
 * @version 2022/12/2 22:39
 */
object LineToRow {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("test").master("local").getOrCreate()
    val frame = spark.createDataFrame(List(User("name1", 1, 11, 2, 22), User("name2", 21, 211, 22, 222)))
    frame.createTempView("tb_test")
    // select name,age,t.hobby from sr2 lateral view explode(split(hobby,'，')) t as hobby
    spark.sql("SELECT concat('1',',','2')").show()
    spark.sql("select * from tb_test").show()
    val sql = "SELECT name,split(t.hobby,';')[0],split(t.hobby,';')[1] FROM tb_test lateral view explode(split(concat(vn1,';',vn2,',',vb1,';',vb2),',')) t as hobby"
    spark.sql(sql).show()
  }

}

case class User(name: String, vn1: Int, vn2: Int, vb1: Int, vb2: Int)