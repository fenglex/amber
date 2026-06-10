package ink.haifeng.scala

// 偏应用函数

/**
 *  偏函数可以提供了简洁的语法，可以简化函数的定义。配合集合的函数式编程，可以让代码更加优雅。
    偏函数被包在花括号内没有match的一组case语句是一个偏函数
    偏函数是PartialFunction[A, B]的一个实例
    A代表输入参数类型
    B代表返回结果类型
 */
object PartialFunction {
  def main(args: Array[String]): Unit = {
    def xxx:PartialFunction[  Any,String] ={
      case "hello"  => "val is hello"
      case x:Int => s"$x...is int"
      case _ => "none"
    }

    println(xxx(44))
    println(xxx("hello"))
    println(xxx("hi"))
  }
}
