package cn.enjoyedu.base1_helloworld;

import cn.enjoyedu.config.BusiConst;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;


import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

/**
 * 消费者 hello world
 */
public class HelloKafkaConsumer {

    public static void main(String[] args) {
        //消费者三个属性必须指定(broker地址清单、key和value的反序列化器)
        Properties properties = new Properties();
        properties.put("bootstrap.servers","119.45.206.237:9092");
        properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        //群组并非完全必须
        properties.put(ConsumerConfig.GROUP_ID_CONFIG,"test1");
        KafkaConsumer<String,String> consumer = new KafkaConsumer<>(properties);
        try {
            //消费者订阅主题（可以多个）
            consumer.subscribe(Collections.singletonList(BusiConst.HELLO_TOPIC));
            while(true){
                //拉取（新版本）
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(5000));
                for(ConsumerRecord<String, String> record:records){
                    //提交偏移量(提交越频繁，性能越差)
                    System.out.println(String.format("topic:%s,分区：%d,偏移量：%d," + "key:%s,value:%s",
                            record.topic(),
                            record.partition(),
                            record.offset(),
                            record.key(),record.value()));
                    //do my work(业务异常，可能进行重试  偏移量，写入主题 异常主题)
                    //打包任务投入线程池
                }
                //提交偏移量
            }
            //通过另外一个线程 consumer. wakeup()
        } finally {
            consumer.close();
        }
    }
}