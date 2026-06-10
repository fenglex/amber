package ink.haifeng.scala

object Test {
  def main(args: Array[String]): Unit = {
    val list = List(1, 2, 3, 4, 5)

    val i = list.map((_, 1))
    println(i)
    for (x <- list.indices) {
      println(x)
    }
  }

}
