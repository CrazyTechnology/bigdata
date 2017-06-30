package com.ming.scala
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.util.control.Breaks._
import scala.io.Source._
import scala.collection.mutable.Map
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


  def lazyTest(): Unit ={
   lazy val  line=fromFile("")
  }


  def exceptionTest(): Unit ={
    try {
      val num=1/0;
    }catch{
      case e1: IllegalArgumentException => println("illegal argument")
      case e2: Exception => println("0 can not be de")
    }finally {
      print("realese  resource!!!")
    }
  }


  def arrayTest(): Unit ={
    var arry=Array("1",2,"jim",56)
    var arry_1=new Array[Int](3)
    arry_1(0)=7
    var arry_2=new Array[String](3)
    var arry_3=new ArrayBuffer[Int]()
    arry_3+=1;
    arry_3+=(2,9,4,8,7)
    for (n<- arry_3){
      println(n)
    }
    println("-------------------------")
    for(n<- 0 until arry_1.length){
      println(arry_1(n))
    }
    println("-------------------------")
    println("SUM ="+arry_3.sum)
    println("MAX ="+arry_3.max)
    println("Data ="+arry_3.mkString("<",",",">"))
  }


  def arrayTest2(): Unit ={
    var a=Array(1,2,3,4,5,6,7);
    var b=for(n<-a) yield  n*n
    for(i<- b)
      println(i)
    println("-----------------------------")
    var c=b.filter(_%2==0).map(2*_)
    for(i<- c)
      println(i)
  }

  def mapTest(): Unit ={
    val map= Map("tom"->"beijing","jack"->"shanghai")
   for((key,value)<-map)
    println(key+" "+value);
    var n=mutable.LinkedHashMap()
  }



  def littleTest(): Unit ={
    val a=new ArrayBuffer[Int]()
    a+=(1,2,3,4,5,-1,-3,8,-5,-8)
    var foundFirstNaviget=false;
    var length=a.length;
    var index=0;
    while(index<length){
      if(a(index)<0){
        foundFirstNaviget=true
        a.remove(index)
        length-=1
      }
      else{
        foundFirstNaviget=false
        index+=1
      }

    }

    for(i <-a){
      println(i)
    }

  }



  def tupleTest: Unit ={
    val t=("tom","jack","java")
    val s=("tom_1","jack_1","java_1")
    //val st= s.zip(t)
   // println(st)

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
     // println(sum2(1 to 5:_*))
    //exceptionTest
   // arrayTest
    //arrayTest2
    //mapTest
    //littleTest
    //tupleTest
   var hell=new  TestClass("jim")


  }

}
