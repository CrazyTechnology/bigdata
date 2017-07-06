package com.ming.scala.test

/**
  * Created by root on 6/30/17.
  */
class TestClass(name:String="tom",age:Int=45) {
  def this(name:String){
    this()
    println("name "+name)
  }
  println("name is "+name+"and age is"+age)
}

