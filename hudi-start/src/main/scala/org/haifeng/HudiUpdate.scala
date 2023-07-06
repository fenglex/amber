package org.haifeng

import org.apache.hudi.DataSourceWriteOptions
import org.apache.hudi.config.HoodieWriteConfig
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

object HudiUpdate {
  def main(args: Array[String]): Unit = {
    val session: SparkSession = SparkSession.builder().master("local").appName("updataDataToHudi")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .getOrCreate()

    //读取修改数据
    val path = "/Users/haifeng/workspace/amber/hudi-start/data/updatedata.json"
    val updateDataDF: DataFrame = session.read.json(path)

    //向Hudi 更新数据
    updateDataDF.write.format("org.apache.hudi") //或者直接写hudi
      .option(DataSourceWriteOptions.RECORDKEY_FIELD_OPT_KEY, "id")
      .option(DataSourceWriteOptions.PRECOMBINE_FIELD_OPT_KEY, "data_dt")
      .option(DataSourceWriteOptions.PARTITIONPATH_FIELD_OPT_KEY, "loc")
      .option("hoodie.insert.shuffle.parallelism", "2")
      .option("hoodie.upsert.shuffle.parallelism", "2")
      .option(HoodieWriteConfig.TABLE_NAME, "person_infos")
      .mode(SaveMode.Append)
      .save("/Users/haifeng/workspace/amber/hudi-start/save_data/person_infos")


    //查询数据
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
