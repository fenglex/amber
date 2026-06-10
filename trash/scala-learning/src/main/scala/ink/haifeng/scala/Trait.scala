package ink.haifeng.scala

object Trait {
  def main(args: Array[String]): Unit = {

    val p = new Person("zhangsan")
    p.hello()
    p.say()
    p.ku()
    p.haiRen()
    println(p.first())

  }

}

trait God {
  def say(): Unit = {
    println("god...say")
  }

  def first(): String = {
    "first1"
  }
}

trait Mg {
  def ku(): Unit = {
    println("mg...say")
  }

  def haiRen(): Unit

//  def first(): String = {
//    "first2"
//  }
}

class Person(name: String) extends God with Mg {

  def hello(): Unit = {
    println(s"$name say hello")
  }

  override def haiRen(): Unit = {
    println("ziji shixian ....")
  }
  
}