package ink.haifeng.scala

object Test01 {
  def main(args: Array[String]): Unit = {
    val user = new User("122")
    val user2 = new User(12, "2")
  }

}

// 只有类名的构造函数参数可以设置为var（类名构造器中的参数就是类的成员属性）
class User(var name: String) {

  // 此处的参数不能设置var (此处的参数并不是类的成员属性)
  def this(age: Int, address: String) {
    // 此处必须调用
    this("name：" + s"$age+$address")
  }

  // new对象时候会执行一次
  println(s"这是用户名$name,")
  // 此处调用age会报错
  //print(s"这是用户名$name,$age")

  def printInfo(): Unit = {
    print(s"这是用户名$name")
  }

}
