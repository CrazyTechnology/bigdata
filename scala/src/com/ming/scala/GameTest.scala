package com.ming.scala
import scala.util.control.Breaks._
/**
  * Created by root on 6/29/17.
  */
object GameTest {

  def Game(): Unit ={
    val name=readLine("input you name");
    println("welcom to our word "+name+" and input your age")
    val age=readInt();
    if(age > 18)
      printf("hello %s you are %d years old and  leagel to our home",name,age)
    else
      printf("hello %s your are %d and too young too simpel",name,age)
  }

  def num(num:Int): Unit ={
    for(n <- 0 to num)
      println(n);  //0...num
    for(n<-0 until num)
      println(n) //0...num-1
  }

  def whileTest(): Unit ={
    var n = 30
    while(n > 0) {
      println(n)
      n -= 1
      if(n==10)
        break
    }
  }

  def MultiPlay(): Unit ={
    for(i <- 1 to 9; j <- 1 to 9 if i==2&&j==6) {
      if(j == 9) {
        println(i * j) //j==9  change another line
      } else {
        print(i * j + " ")
      }
    }

    for(i <- 1 to 10 if i%2==0){
      print(i)
    }
    println("-----------------")

    for(i <- 1 to 10) yield i

  }


  def feb(num:Int):Int={
    if(num <= 1) 1
    else feb(num-1)+feb(num-2)

  }

  def test(name:String)= print("hello "+name)


  def defaultValue(name:String,age:Int=20,sex:String="man"): Unit ={
    println("hello "+name+" you are "+age+"years old and you are a "+sex)
  }
  def defaultValue1(name:String="liu dehua ",age:Int=20,sex:String="man"): Unit ={
    println("hello "+name+" you are "+age+"years old and you are a "+sex)
  }

  def total(nums:Int *):Int={
   var result=0
    for(num<-nums){
      result=result+num
    }
  result

  }

  def sum2(nums: Int*): Int = {
    if (nums.length == 0) 0
    else nums.head + sum2(nums.tail: _*)
  }


  def main(args: Array[String]): Unit = {
    //Game()
    //num(5)
    //whileTest
   // MultiPlay
//    var total=feb(10);
//    print(total);
//    test("xiao hei")
   // defaultValue(name="xiaoli" ,sex="woman",age=15)
   println(sum2(1 to 5:_*))
  }

}
