package ink.haifeng.scala

import java.util
import scala.collection.{immutable, mutable}
import scala.collection.mutable.ListBuffer

object Collections {
  def main(args: Array[String]): Unit = {

    val listJava = new util.LinkedList[String]()
    listJava.add("list")

    // 1 数组
    // Java 中的泛型是<> scala中的泛型是[],所以数组用(n)
    // val 约等于final 不可变描述的是val指定的引用的值（值：字面值，地址）
    val arr01 = Array[Int](1, 2, 3, 4)
    arr01(1) = 10
    println(arr01(1))
    for (e <- arr01) {
      println(e)
    }
    arr01.foreach(println)


    // 2. 链表
    // scala 的collection中有两个包，immutable,mutable,默认是不可变的immutable
    println("----------------------链表------------- ")
    val list01 = List(1, 2, 3, 4, 5, 6, 7, 8)
    //    for (e <- list01) {
    //      println(e)
    //    }
    //    list01.foreach(println)

    val list02 = new ListBuffer[Int]()
    list02.+=(1)
    list02.+=(2)
    list02.+=(33)
    list02 += 1
    list02 ++= list02 //两个集合相加
    list02.foreach(println)

    println("--------------")
    val list03 = new ListBuffer[Int]()
    list03 += 1
    list03 += 2
    list03 += 3
    list03.foreach(println)
    list03 ++: list03
    list03.foreach(println)

    /**
     * scala中的:: , +:, :+, :::, +++, 等操作;
     */
    println("--------------")
    val list = List(1, 2, 3)
    // :: 用于的是向队列的头部追加数据,产生新的列表, x::list,x就会添加到list的头部
    println(4 :: list) //输出: List(4, 1, 2, 3)
    // .:: 这个是list的一个方法;作用和上面的一样,把元素添加到头部位置; list.::(x);
    println(list.::(5)) //输出: List(5, 1, 2, 3)
    // :+ 用于在list尾部追加元素; list :+ x;
    println(list :+ 6) //输出: List(1, 2, 3, 6)
    // +: 用于在list的头部添加元素;
    val list2 = "A" +: "B" +: Nil //Nil Nil是一个空的List,定义为List[Nothing]
    println(list2) //输出: List(A, B)
    // ::: 用于连接两个List类型的集合 list ::: list2
    println(list ::: list2) //输出: List(1, 2, 3, A, B)
    // ++ 用于连接两个集合，list ++ list2
    println(list ++ list2) //输出: List(1, 2, 3, A, B)


    println("------------Set--------------")

    val set01: Set[Int] = Set(1, 2, 3, 4, 5)

    import scala.collection.mutable.Set
    val set02: mutable.Set[Int] = mutable.Set(11, 22, 33, 44)
    set02.add(55)

    println("----------tuple--------")
    val t2 = Tuple2(11, "asdfas")
    val t3 = Tuple3(1, 2, 3)

    // 变量tuple的值
    t3.productIterator.foreach(println)

    println("--------------map---------------")
    val map01: Map[String, Int] = Map(("a", 33), "b" -> 22, ("c", 11), ("d", 44))
    val keys = map01.keys
    println(map01.get("a").get)
    // 报错 None.get
    //println(map01.get("x").get)
    println(map01.get("a").getOrElse("hello world"))
    println(map01.get("w").getOrElse("hello world"))
    println(map01.getOrElse("a", "hello world"))
    println(map01.getOrElse("w", "hello world"))

    val map02: mutable.Map[String, Int] = scala.collection.mutable.Map(("a", 11), ("b", 22))
    map02.put("c", 22)

    println("--------------艺术-------------")
    val listYs = List(1, 2, 3, 4, 5, 6)
    val listMap = listYs.map((x: Int) => x + 10)
    listMap.foreach(println)

    println("--------------艺术-升华------------")

    val listStr = List(
      "hello world",
      "hello msb",
      "good idea"
    )
    val flatMap = listStr.flatMap((x: String) => x.split(" "))
    flatMap.foreach(println)
    val mapList = flatMap.map((_, 1))
    mapList.foreach(println)


    println("--------------艺术-再-升华------------")
    val iter: Iterator[String] = listStr.iterator //什么是迭代器，为什么会有迭代器模式？  迭代器里不存数据！

    val iterFlatMap = iter.flatMap((x: String) => x.split(" "))
    //    iterFlatMap.foreach(println)

    val iterMapList = iterFlatMap.map((_, 1))

    while (iterMapList.hasNext) {
      val tuple: (String, Int) = iterMapList.next()
      println(tuple)
    }



    //    iterMapList.foreach(println)

    //1.listStr真正的数据集，有数据的
    //2.iter.flatMap  没有发生计算，返回了一个新的迭代器

  }


}

