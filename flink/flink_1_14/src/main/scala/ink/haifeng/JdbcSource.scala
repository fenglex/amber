package ink.haifeng

import cn.hutool.db.Db
import cn.hutool.db.ds.simple.SimpleDataSource
import cn.hutool.db.handler.RsHandler
import org.apache.flink.streaming.api.functions.source.{RichSourceFunction, SourceFunction}
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._

import java.sql.ResultSet
import scala.collection.mutable.ArrayBuffer

object JdbcSource {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.addSource(new JdbcSource()).print("test")
    env.execute()

  }

}

case class UserInfo(id: Int, name: String, age: Int)

class JdbcSource extends RichSourceFunction[UserInfo] {

  override def cancel(): Unit = {

  }

  override def run(sourceContext: SourceFunction.SourceContext[UserInfo]): Unit = {
    val source = new SimpleDataSource("jdbc:mysql://192.168.31.95:3306/tb_test?useUnicode=true&characterEncoding=utf-8", "root", "12345")
    val db = Db.use(source)
    while (true) {
      val infos = db.query("select * from tb_user", new RsHandler[ArrayBuffer[UserInfo]] {
        override def handle(rs: ResultSet): ArrayBuffer[UserInfo] = {
          val array = new ArrayBuffer[UserInfo]()
          while (rs.next()) {
            val info = UserInfo(rs.getInt("id"), rs.getString("name"), rs.getInt("age"))
            array.append(info)
          }
          array
        }
      })
      infos.foreach(sourceContext.collect)
      Thread.sleep(5000)
    }
  }
}