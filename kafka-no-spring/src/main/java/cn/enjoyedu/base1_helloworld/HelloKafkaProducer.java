package cn.enjoyedu.base1_helloworld;

import cn.enjoyedu.config.BusiConst;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

/**
 * 生产者 hello world
 */
public class HelloKafkaProducer {

    public static void main(String[] args) {

        //生产者三个属性必须指定(broker地址清单、key和value的序列化器)
        Properties properties = new Properties();
        properties.put("bootstrap.servers","119.45.206.237:9092");
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        KafkaProducer<String,String> producer = new KafkaProducer<>(properties);
        try {
            ProducerRecord<String,String> record;
            try {
                //发送4条消息
                for(int i=0;i<4;i++){
                    record = new ProducerRecord<>(BusiConst.HELLO_TOPIC, String.valueOf(i),"lison");
                    producer.send(record);//发送并忘记（重试会有）
                    System.out.println(i+"，message is sent");
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        } finally {
            producer.close();
        }
    }
}