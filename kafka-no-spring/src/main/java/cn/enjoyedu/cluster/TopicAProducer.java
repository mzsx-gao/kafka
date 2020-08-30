package cn.enjoyedu.cluster;

import cn.enjoyedu.config.BusiConst;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

/**

 * 类说明：kafka生产者(向集群发送消息)
 */
public class TopicAProducer {

    public static void main(String[] args) {
        //TODO 生产者三个属性必须指定(broker地址清单、key和value的序列化器)
        Properties properties = new Properties();
        //kafka服务器地址配置了多条，同时也有非首领分区的broker
        properties.put("bootstrap.servers","127.0.0.1:9092,127.0.0.1:9093");
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        KafkaProducer<String,String> producer = new KafkaProducer<String, String>(properties);
        try {
            ProducerRecord<String,String> record;
            try {
                //TODO 发送4条消息
                for(int i=0;i<4;i++){
                    record = new ProducerRecord<String,String>("TopicA", null,"lison");
                    producer.send(record);//发送并发忘记（重试会有）
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
