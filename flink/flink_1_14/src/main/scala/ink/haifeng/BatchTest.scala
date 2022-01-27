package ink.haifeng


import org.apache.flink.streaming.api.scala._

object BatchTest {
  def main(args: Array[String]): Unit = {
    val path="/Users/haifeng/workspace/Projects/amber/flink/flink_1_14/data/data.csv";
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.readTextFile(path).map(e => {
      val strings = e.split(",")
      VisitData(strings(0), strings(1), strings(2), strings(3).toInt, strings(4))
    }).k
    env.execute()
  }

}

case class VisitData(region: String, device: String, uid: String, time: Int, dateTime: String)