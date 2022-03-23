package ink.haifeng.table


import org.apache.flink.api.scala._
import org.apache.flink.table.api._
import org.apache.flink.table.api.bridge.scala._

/**
 * @author haifeng
 */
object TableTest {
  def main(args: Array[String]): Unit = {
    val settings = EnvironmentSettings.newInstance().inStreamingMode().build()

    val tabEnv = TableEnvironment.create(settings)

    val order: Table = tabEnv.fromValues(DataTypes.ROW(
      DataTypes.FIELD("id", DataTypes.INT()),
      DataTypes.FIELD("name", DataTypes.STRING())
    ),
      row(1, "order1"),
      row(2, "order2"))

    val item = tabEnv.fromValues(DataTypes.ROW(
      DataTypes.FIELD("order_id", DataTypes.INT()),
      DataTypes.FIELD("item", DataTypes.STRING())
    ),
      row(1, "item1"),
      row(1, "item2"),
      row(2, "item21"),
      row(2, "item22"))
    tabEnv.createTemporaryView("tb_order", order)
    tabEnv.createTemporaryView("tb_item", item)
    tabEnv.executeSql("select t1.id,t1.name,t2.item from  tb_order t1 left join tb_item t2 on t1.id=t2.order_id").print()

    Thread.sleep(1 * 60 * 1000)
  }

}
