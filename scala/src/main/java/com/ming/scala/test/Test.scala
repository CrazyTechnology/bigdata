package com.ming.scala.test

/**
  * Created by root on 6/28/17.
  */
object Test {

  def sayHello(name:String,age:String)={
    print("hello "+name+" you are "+age+" years old")
  }

  def playGame(name:String,age:Int=0){
    print("welcom to our word "+name);
    if(age>18)
      print("you are "+age+"years old")
    else
      print("you are too young too simaple")

  }


  def sum(nums:Int *)={
    var total=0;
    for(num <-nums)
      total+=num;
    total
  }

  def main(args: Array[String]): Unit = {
    var array=Array(1,2,3,4,5)
    var b=array.find(num=>num%2==0)
    var a=array.filter(num=>num%2==0)
    System.out.print(a.length)
  }
}
