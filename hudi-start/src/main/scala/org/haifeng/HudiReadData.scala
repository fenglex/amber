package org.haifeng

import org.apache.spark.sql.{DataFrame, SparkSession}

object HudiReadData {
  def main(args: Array[String]): Unit = {
    val session: SparkSession = SparkSession.builder().master("local").appName("queryDataFromHudi")
      .getOrCreate()
    //读取的数据路径下如果有分区，会自动发现分区数据,需要使用 * 代替，指定到parquet格式数据上层目录即可。
    val frame: DataFrame = session.read.format("org.apache.hudi").load("/Users/haifeng/workspace/amber/hudi-start/save_data/person_infos/*/*")
    frame.createTempView("personInfos")

    //查询结果
    val result = session.sql(
      """
        | select * from personInfos
      """.stripMargin)

    result.show(false)
  }

}
