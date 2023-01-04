package ink.haifeng.spark

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.udf

/**
 * DataCheck
 *
 * @author haifeng
 * @version 2022/12/26 9:00
 */
object DataCheck {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().master("local").appName("dataCheck").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    import spark.implicits._
    val frame = spark.read
      .option("delimiter", "\001")
      .option("header", "false")
      .csv("D:\\Project\\amber\\spark\\spark-learning-scala\\data")
    frame.show()
    frame.createTempView("test")
    import org.apache.spark.sql.functions.udf
    //val monthDiff = udf((a1: Int, a2: Int) -> a1/a2)
    val monthDiff = (a1: Int, a2: Int) => {
      val year1 = a1 / 10000
      val year2 = a2 / 10000
      val month1 = (a1 / 100) % 100
      val month2 = (a2 / 100) % 100
      (12 - month1) + 1 + (year2 - year1 - 1) * 12 + month2
    }
    spark.udf.register("month_diff", monthDiff)
    //spark.udf.register()

    println(monthDiff(20110331, 20220930))

    val sql =
      """
        |
        |select * from (
        |select
        |_c0 as code,
        |min(_c1) as start,
        |max(_c1) as end,
        |month_diff(min(_c1),max(_c1)) as diff,
        |count(*) as ct
        |from test   group by _c0) where ct!=diff
        |""".stripMargin
    // spark.sql(sql).show(50, false)

    //TD00000421_cumu_pop
    //TD00000827_abs_chg_pop
    val code = "TD00000827_abs_chg_pop"
    spark.sql(s"select _c0,year,count(*) from (select *,cast(_c1/10000 as int) as year from test where _c0='$code') group by _c0,year order by year").show(1000, false)

    spark.sql(s"select * from test where _c0='$code' order by _c1").show(1000, false)

    val sql2 =
      """
        |
        |select count(*) from (
        |select
        |_c0 as code,
        |min(_c1) as start,
        |max(_c1) as end,
        |month_diff(min(_c1),max(_c1)) as diff,
        |count(*) as ct
        |from test   group by _c0) where ct!=diff and code not like 'TD%'
        |""".stripMargin
    spark.sql(sql2).show()
    spark.sql("select count(distinct(_c0)) from test ").show()
    //spark.sql("select * from test where _c0='AC001_roe_ttm_pop1' order by _c1").show(1000, false)
  }

}
