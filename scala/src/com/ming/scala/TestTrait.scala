package com.ming.scala

/**
  * Created by root on 7/1/17.
  */
trait TestTrait {
  def sayHello(name:String)

  def logger(msg:String)=println("this is log"+msg)
  val age:Int
}

trait makeFriends{
  def makeFriedns(p:Person)
}

class Person(val name:String) extends TestTrait with  makeFriends{
  override def sayHello(name: String): Unit = println("hello "+name+" and "+age)

  override def makeFriedns(p: Person): Unit = println("I am "+name+" I want make friends with you"+p.name)

  override val age: Int = 25
}

object Test1{
  def main(args: Array[String]): Unit = {
    val p=new Person("xiaowang")
    val p2=new Person("xiaoli")
    p.sayHello("xiaoli")
    p.makeFriedns(p2)
    p.logger("hhh")
  }
}