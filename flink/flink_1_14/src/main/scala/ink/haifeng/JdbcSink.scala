package ink.haifeng


import cn.hutool.db.Db
import cn.hutool.db.ds.simple.SimpleDataSource
import cn.hutool.db.handler.RsHandler
import org.apache.flink.streaming.api.functions.source.{RichSourceFunction, SourceFunction}
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._


object JdbcSink {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.addSource(new JdbcSource()).print("test")
    env.execute()
  }

}
