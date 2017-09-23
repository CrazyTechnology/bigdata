package com.ming.integrated.kafka;

import com.ming.integrated.util.PropertiesUtils;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import kafka.serializer.StringEncoder;

import java.util.Properties;
import java.util.concurrent.TimeUnit;

/**
 * Created by ming on 2017/9/22.
 */
public class KafkaProducer extends Thread {
    private  String topic;
    public KafkaProducer(String topic){
        super();
        this.topic = topic;
    }
    public void run() {
        Producer producer = createProducer();
        int i=0;
        while(true){
            producer.send(new KeyedMessage<Integer, String>(topic, "message: " + i++));
            try {
                TimeUnit.SECONDS.sleep(1);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
    private Producer createProducer() {
        Properties properties = new Properties();
        properties.put("zookeeper.connect", PropertiesUtils.ZOOKEEPER_URL);//声明zk
        properties.put("serializer.class", StringEncoder.class.getName());
        properties.put("metadata.broker.list", PropertiesUtils.BROKER_LIST);// 声明kafka broker
        return new Producer<Integer, String>(new ProducerConfig(properties));
    }
    public static void main(String[] args) {
        new KafkaProducer("bigdata").start();// 使用kafka集群中创建好的主题

    }
}
