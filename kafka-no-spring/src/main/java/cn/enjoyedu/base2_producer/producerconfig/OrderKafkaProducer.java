package cn.enjoyedu.base2_producer.producerconfig;

import cn.enjoyedu.config.BusiConst;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

/**
 * 类说明：kafka生产者消息顺序保证
 */
public class OrderKafkaProducer {

    public static void main(String[] args) {
        //生产者三个属性必须指定(broker地址清单、key和value的序列化器)
        Properties properties = new Properties();
        properties.put("bootstrap.servers","127.0.0.1:9092");
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        //顺序消息的保证（只有一个分区、）
        //properties.put("retries",0); //重发消息次数（设置为0）

        //在阻塞之前，客户端将在单个连接上发送的未确认请求的最大数目
        //max.in.flight.request.per.connection 设为1，这样在生产者尝试发送第一批消息时，就不会有其他的消息发送给broker
        //这个值默认是5
        properties.put("max.in.flight.requests.per.connection",1);


        KafkaProducer<String,String> producer = new KafkaProducer<>(properties);
        try {
            ProducerRecord<String,String> record;
            try {
                //发送4条消息
                for(int i=0;i<4;i++){
                    record = new ProducerRecord<>(BusiConst.HELLO_TOPIC, String.valueOf(i),"lison");
                    producer.send(record);
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