package com.ming.common.utils

import com.typesafe.scalalogging.Logger
import org.I0Itec.zkclient.ZkClient
import org.apache.commons.codec.StringDecoder
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.slf4j.LoggerFactory

/**
  * Created by ming on 18-6-19.
  */
object KafkaUtil {
  val logger = Logger(LoggerFactory.getLogger("KafkaUtil"))
  def getKafkaStream(zookeeper:String,groupId:String,topic:String,kafkaParams:Map[String,String],ssc:StreamingContext): InputDStream[(String, String)] ={
        val topicDir=new ZKGroupTopicDirs(groupId,topic)
        val zkTopicPath=s"${topicDir.consumerOffsetDir}"
        val zkClient=new ZkClient(zookeeper)
        val children=zkClient.countChildren(zkTopicPath)

        var adRealtimeLogDStream: InputDStream[(String, String)] = null
        if(children>0){
            logger.warn("从zookeeper中读取offset位置")

        }else{
          logger.warn("首次消费，直接创建")
          adRealtimeLogDStream=KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, Set(topic))

        }










        adRealtimeLogDStream


      }
}
