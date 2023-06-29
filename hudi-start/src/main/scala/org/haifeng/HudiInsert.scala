package org.haifeng

import org.apache.hudi.DataSourceWriteOptions
import org.apache.hudi.config.HoodieWriteConfig
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

object HudiInsert {
  def main(args: Array[String]): Unit = {
    val session: SparkSession = SparkSession.builder().master("local").appName("insertDataToHudi")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .getOrCreate()

    //关闭日志
    //    session.sparkContext.setLogLevel("Error")

    //创建DataFrame
    val insertDF: DataFrame = session.read.json("file:///D:\\2018IDEA_space\\SparkOperateHudi\\data\\jsondata.json")


    //将结果保存到hudi中
    insertDF.write.format("org.apache.hudi") //或者直接写hudi
      //设置主键列名称
      .option(DataSourceWriteOptions.RECORDKEY_FIELD_OPT_KEY, "id")
      //当数据主键相同时，对比的字段，保存该字段大的数据
      .option(DataSourceWriteOptions.PRECOMBINE_FIELD_OPT_KEY, "data_dt")
      //并行度设置，默认1500
      .option("hoodie.insert.shuffle.parallelism", "2")
      .option("hoodie.upsert.shuffle.parallelism", "2")
      //表名设置
      .option(HoodieWriteConfig.TABLE_NAME, "person_infos")
      .mode(SaveMode.Overwrite)
      //注意：这里要选择hdfs路径存储，不要加上hdfs://mycluster//dir
      //将hdfs 中core-site.xml 、hdfs-site.xml放在resource目录下，直接写/dir路径即可，否则会报错：java.lang.IllegalArgumentException: Not in marker dir. Marker Path=hdfs://mycluster/hudi_data/.hoodie\.temp/20210709164730/default/c4b854e7-51d3-4a14-9b7e-54e2e88a9701-0_0-22-22_20210709164730.parquet.marker.CREATE, Expected Marker Root=/hudi_data/.hoodie/.temp/20210709164730
      .save("/hudi_data/person_infos")
  }

}
