package test

import scala.collection.immutable.HashMap


object Test {
  def main(args: Array[String]): Unit = {
    val colors=Map("red"->"#FF0000","black"->"#000000")
    var A:HashMap[Char,Int] = HashMap()
    A += ('I' -> 1)
    A += ('J' -> 5)
    A += ('K' -> 10)
    A +=('L'->13)


    println(15&240)
  }
}
