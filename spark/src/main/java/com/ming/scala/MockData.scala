package com.ming.scala

import org.apache.spark.sql.RowFactory
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types.{DataTypes, StructField}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ListBuffer

object MockData {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("MockData")
    val sc = new SparkContext(conf)
    val hiveContext=new HiveContext(sc)
    val event_id= Array("A099001","A099002","A099003","A099004","A099005","A099007","A099008","A099009","A099011","A099014")
    val rom_version="4.0.0-2017070701-NIGHTLY-userdebug-col"
    val timestamp=1500262947
    val event_data="{}"
    val platform=0;
    val hardware_version="SM919"
    val channel="0099"
    val device_id="{\"imei\":\"990009040009204\"}"
    val app_version="24"
    val data=new ListBuffer[String]
    for ( id <- event_id ) {
      for (i <- 1 until 100001) {
        val str = i+  "\t" +id+"\t"+event_data+"\t"+timestamp+"\t"+platform+"\t"+device_id+"\t"+app_version+"\t"+rom_version+"\t"+hardware_version+"\t"+channel
        data.append(str)
      }
    }

  val rdd= sc.parallelize(data);

    val testdata =rdd.map(line=>{
      val lines=line.split("\t")
      RowFactory.create(lines(0),lines(1),lines(2),lines(3),lines(4),lines(5),lines(6),lines(7),lines(8),lines(9))

    })
    val structField=new ListBuffer[StructField]
    structField.append(DataTypes.createStructField("uid",DataTypes.StringType,false))
    structField.append(DataTypes.createStructField("event_id",DataTypes.StringType,false))
    structField.append(DataTypes.createStructField("event_data",DataTypes.StringType,false))
    structField.append(DataTypes.createStructField("ts",DataTypes.StringType,false))
    structField.append(DataTypes.createStructField("platform",DataTypes.StringType,false))
    structField.append(DataTypes.createStructField("device_id",DataTypes.StringType,false))
    structField.append(DataTypes.createStructField("app_version",DataTypes.StringType,false))
    structField.append(DataTypes.createStructField("rom_version",DataTypes.StringType,false))
    structField.append(DataTypes.createStructField("hardware_version",DataTypes.StringType,false))
    structField.append(DataTypes.createStructField("channel",DataTypes.StringType,false))
    val stype=DataTypes.createStructType(structField.toArray)
   val df= hiveContext.createDataFrame(testdata,stype)
    df.registerTempTable("mock_data")
    //hiveContext.sql("select uid,event_id,event_data,timestamp,platform,device_id,app_version,rom_version,hardware_version,channel from mock_data").show(100)


    hiveContext.sql("insert into table  bi_data.app_event partition (app='A09',year=2017,month=11,day=11) select uid,event_id,event_data,ts,platform,device_id,app_version,rom_version,hardware_version,channel from mock_data")
  }




}
