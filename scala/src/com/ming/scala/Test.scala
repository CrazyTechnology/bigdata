package com.ming.scala

/**
  * Created by root on 6/28/17.
  */
object Test {

  def sayHello(name:String,age:String)={
    print("hello "+name+" you are "+age+" years old")
  }

  def main(args: Array[String]): Unit = {
    print(sayHello("xiaoli","20"))
  }
}
