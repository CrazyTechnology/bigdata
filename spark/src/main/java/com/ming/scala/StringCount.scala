package com.ming.scala

import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._

/**
  * Created by Administrator on 2017-08-09.
  */
class StringCount extends UserDefinedAggregateFunction{
  //inputSchema 指的是输入数据的类型
   def inputSchema: StructType ={
    StructType(Array(StructField("str", StringType, true)))
  }

  //指的是有新的值进来的时候如何处理
   def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    buffer(0)=buffer.getAs[Int](0)+1
  }

  //指的是中间进行数据聚合的时候，所处理的数据类型
   def bufferSchema: StructType = {
    StructType(Array(StructField("count", IntegerType, true)))
  }

  //由于Spark是分布式的，所以一个分组的数据，可能会在不同的节点上进行局部聚合，就是update
  // 但是，最后一个分组，在各个节点上的聚合值，要进行merge，也就是合并
   def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    buffer1(0) = buffer1.getAs[Int](0) + buffer2.getAs[Int](0)
  }

  //为每个数组的数据执行初始化操作
  //MutableAggregationBuffer 可变的聚合缓冲区
   def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0)=0;
  }

   def deterministic: Boolean = {
     true
   }

  //最后，指的是，一个分组的聚合值，如何通过中间的缓存聚合值，最后返回一个最终的聚合值
   def evaluate(buffer: Row): Any = {
     buffer.getAs[Int](0)
   }

  //函数返回值的类型
   def dataType: DataType = {
    IntegerType
  }
}