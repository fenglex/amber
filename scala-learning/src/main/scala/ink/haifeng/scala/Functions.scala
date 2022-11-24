package ink.haifeng.scala

import java.util.Date

object Functions {
  def main(args: Array[String]): Unit = {

  }

  //成员方法
  def ooxx(): Unit = {
    println("hello object")
  }

  //参数：必须给出类型，是val
  //class 构造，是var，val
  def fun03(a: Int): Unit = {
    println(a)
  }

  println("-------默认值函数--------")

  def defaultArgs(a: Int = 8, b: String = "valueb"): Unit = {
    println(s"$a\t$b")
  }

  defaultArgs(22)
  defaultArgs(b = "ooxx")
  println("-------4.匿名函数----------")
  //函数是第一类值
  //函数：
  //1，签名 ：(Int,Int)=>Int ：  （参数类型列表）=> 返回值类型
  //2，匿名函数： (a:Int,b:Int) => { a+b }  ：（参数实现列表）=> 函数体

  var xx: Int = 3
  var yy: (Int, Int) => Int = (a: Int, b: Int) => {
    a + b
  }
  val w: Int = yy(3, 4)
  println(w)

  println("--------5.嵌套函数---------")

  def fun06(a: String): Unit = {
    def fun05(): Unit = {
      println(a)
    }
  }

  fun06("hallo")
  println("--------6.偏应用函数---------")

  def fun07(date: Date, tp: String, msg: String): Unit = {
    println(s"$date\t$tp\t$msg")
  }

  // 可以快速生成一个新的函数

  var info = fun07(_: Date, "info", _: String)
  var error = fun07(_: Date, "error", _: String)
  info(new Date, "ok")
  error(new Date, "error...")

  println("--------7.可变参数---------")

  def fun08(a: Int*): Unit = {
    for (i <- a) {
      println(i)
    }
  }

  fun08(2)
  fun08(1, 2, 3, 4, 5, 6)

  println("--------8.高阶函数---------")

  //函数作为参数，函数作为返回值
  //函数作为参数
  def computer(a: Int, b: Int, f: (Int, Int) => Int): Unit = {
    val res: Int = f(a, b)
    println(res)
  }

  computer(3, 4, (x, y) => {
    x + y
  })
  computer(4, 5, (x, y) => {
    x * y
  })
  computer(5, 5, _ * _)

  //函数作为返回值：
  def factory(i: String): (Int, Int) => Int = {
    // 嵌套函数
    def plus(x: Int, y: Int): Int = {
      x + y
    }

    if (i.equals("+")) {
      plus
    } else {
      (x: Int, y: Int) => {
        x * y
      }
    }
  }

  computer(3, 8, factory("-"))

  println("--------9.柯里化---------")

  def fun09(a: Int)(b: Int)(c: String): Unit = {
    println(s"$a\t$b\t$c")
  }
  fun09(3)(8)("柯里化")

  def fun10(a: Int*)(b: String*): Unit = {
    a.foreach(println)
    b.foreach(println)
  }

  fun10(1, 2, 3)("sdfs", "sss")

  println("--------*.方法---------")

  //方法不想执行，赋值给一个引用  方法名+空格+下划线
  val funa = ooxx
  println(funa)
  val func = ooxx _
  func()

  //语法 ->  编译器  ->  字节码   <-  jvm规则
  //编译器，衔接 人  机器
  //java 中 +： 关键字
  //scala中+： 方法/函数
  //scala语法中，没有基本类型，所以你写一个数字  3  编辑器/语法，其实是把 3 看待成Int这个对象
  //    3 + 2
  //    3.+(2)
  //    3:Int
}
