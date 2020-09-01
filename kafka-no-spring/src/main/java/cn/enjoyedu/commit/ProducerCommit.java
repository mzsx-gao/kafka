package cn.enjoyedu.commit;

import cn.enjoyedu.config.BusiConst;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

/**
 * 类说明：
 */
public class ProducerCommit {

    private static KafkaProducer<String, String> producer = null;

    public static void main(String[] args) {
        /*发送配置的实例*/
        Properties properties = new Properties();
        /*broker的地址清单*/
        properties.put("bootstrap.servers", "119.45.206.237:9092");
        /*key的序列化器*/
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        /*value的序列化器*/
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        /*消息生产者*/
        producer = new KafkaProducer<>(properties);
        try {
            /*待发送的消息实例*/
            ProducerRecord<String, String> record;
            try {
                for (int i = 0; i < 50; i++) {
                    record = new ProducerRecord<>(BusiConst.CONSUMER_COMMIT_TOPIC, "key" + i, "value" + i);
                    /*发送消息--发送后不管*/
                    producer.send(record);
                    System.out.println("数据[" + record + "]已发送。");
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        } finally {
            producer.close();
        }
    }
}