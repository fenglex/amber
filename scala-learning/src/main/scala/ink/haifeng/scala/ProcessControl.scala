package ink.haifeng.scala

import scala.collection.immutable

object ProcessControl {
  def main(args: Array[String]): Unit = {
    var a = 1
    if (a > 0) {
      println(s"$a>0")
    } else {
      println(s"$a<0")
    }
    println("--------------------")
    while (a < 5) {
      println(s"cur->$a")
      a += 1
    }
    println("--------------------")
    // 遍历过程中增加逻辑判断
    val range = 1 until 10
    for (i <- range if (i % 2 == 0)) {
      println(i)
    }
    println("--------------------")
    // 双重循环打印乘法表
    var num = 0
    for (i <- 1 to 9; j <- 1 to 9 if (j <= i)) {
      num += 1;
      if (j <= i) print(s"$i*$j=${i * j}\t")
      if (j == i) println()
    }
    println("--------------------")
    // yield 收集返回
    val seqss: immutable.IndexedSeq[Int] = for (i <- 1 to 10) yield {
      var x = 8
      i + x
    }
    for (i <- seqss) {
      println(i)
    }
  }
}
