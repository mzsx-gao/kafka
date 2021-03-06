package cn.enjoyedu.base3_consumer.consumergroup;

import cn.enjoyedu.config.BusiConst;
import cn.enjoyedu.config.KafkaConst;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

/**
 * 类说明：主题"consumer-group-test"建立两个分区
 */
public class TestGroupProducer {

    private static KafkaProducer<String,String> producer = null;

    public static void main(String[] args) {
        /*发送配置的实例*/
        Properties properties = KafkaConst.producerConfig(StringSerializer.class, StringSerializer.class);
        /*消息生产者*/
        producer = new KafkaProducer<>(properties);
        try {
            /*待发送的消息实例*/
            ProducerRecord<String,String> record;
            try {
                for(int i=0;i<50;i++){
                    record = new ProducerRecord<>(BusiConst.CONSUMER_GROUP_TOPIC, "key"+i,"value"+i);
                    /*发送消息--发送后不管*/
                    producer.send(record);
                    System.out.println("数据["+record+"]已发送。");
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        } finally {
            producer.close();
        }
    }
}