package ink.haifeng


import org.apache.flink.streaming.api.scala._

object BatchTest {
  def main(args: Array[String]): Unit = {
    val path = "/Users/haifeng/workspace/Projects/amber/flink/flink_1_14/data/visit_data.csv";
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val dataStream = env.readTextFile(path).map(e => {
      val strings = e.split(",")
      VisitData(strings(0), strings(1), strings(2), strings(3).toInt, strings(4))
    })
//    dataStream.assignAscendingTimestamps(_.time * 1000).windowAll()
    env.execute()
  }

}

case class VisitData(region: String, device: String, uid: String, time: Int, dateTime: String)