package ink.haifeng.scala

object MatchTest {

  def main(args: Array[String]): Unit = {
    val tup: (Double, Int, String, Boolean, Int) = (1.0, 88, "abc", false, 44)


    // 可以写成一下形式
//    val res: Iterator[Unit] = tup.productIterator.map {
//      case x@1 => println(s"$x...is 1")
//      case x@88 => println(s"$x ...is 88")
//      case x@false => println(s"$x...is false")
//      case w: Int if w > 50 => println(s"$w...is  > 50")
//      case _ => println("wo ye bu zhi dao sha lei xing ")
//    }

    val res: Iterator[Unit] = tup.productIterator.map(
      (x) => {
        x match {
          case 1 => println(s"$x...is 1")
          case 88 => println(s"$x ...is 88")
          case false => println(s"$x...is false")
          case w: Int if w > 50 => println(s"$w...is  > 50")
          case _ => println("wo ye bu zhi dao sha lei xing ")
        }
      }
    )
    while (res.hasNext) println(res.next())
  }
}
