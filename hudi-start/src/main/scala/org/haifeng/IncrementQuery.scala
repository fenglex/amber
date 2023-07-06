package org.haifeng

import org.apache.hudi.DataSourceReadOptions
import org.apache.spark.sql.{DataFrame, SparkSession}

object IncrementQuery {
  def main(args: Array[String]): Unit = {
    val session: SparkSession = SparkSession.builder().master("local").appName("updataDataToHudi")
      .getOrCreate()

    //关闭日志
    session.sparkContext.setLogLevel("Error")

    //导入隐式转换
    import session.implicits._

    //查询全量数据,查询对应的提交时间，找出倒数第二个时间
    val basePath = "/Users/haifeng/workspace/amber/hudi-start/save_data/person_infos"
    session.read.format("hudi").load(basePath + "/*/*").createTempView("personInfos")

    val df: DataFrame = session.sql("select distinct(_hoodie_commit_time) as commit_time from personInfos order by commit_time desc")
    //这里获取由大到小排序的第二个值
    val dt: String = df.map(row => {
      row.getString(0)
    }).collect()(1)

    //增量查询
    val result: DataFrame = session.read.format("hudi")

      /**
       * 指定数据查询方式，有以下三种：
       * val QUERY_TYPE_SNAPSHOT_OPT_VAL = "snapshot"    -- 获取最新所有数据 , 默认
       * val QUERY_TYPE_INCREMENTAL_OPT_VAL = "incremental"  --获取指定时间戳后的变化数据
       * val QUERY_TYPE_READ_OPTIMIZED_OPT_VAL = "read_optimized"  -- 只查询Base文件中的数据
       *
       * 1) Snapshot mode (obtain latest view, based on row & columnar data)
       * 2) incremental mode (new data since an instantTime)
       * 3) Read Optimized mode (obtain latest view, based on columnar data)
       *
       * Default: snapshot
       */
      .option(DataSourceReadOptions.QUERY_TYPE_OPT_KEY, DataSourceReadOptions.QUERY_TYPE_INCREMENTAL_OPT_VAL)
      //必须指定一个开始查询的时间，不指定报错
      .option(DataSourceReadOptions.BEGIN_INSTANTTIME_OPT_KEY, dt)
      .load(basePath + "/*/*")

    result.show(false)
  }

}
