package cn.enjoyedu.base2_producer.selfpartition;

import cn.enjoyedu.config.BusiConst;
import cn.enjoyedu.config.KafkaConst;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.concurrent.Future;

/**
 * 类说明：可以和KafkaFutureProducer比较分区结果
 */
public class SelfPartitionProducer {

    private static KafkaProducer<String, String> producer = null;

    public static void main(String[] args) {
        /*消息生产者*/
        Properties properties = KafkaConst.producerConfig(StringSerializer.class, StringSerializer.class);
        /*使用自定义的分区器*/
        properties.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, "cn.enjoyedu.base2_producer.selfpartition" +
            ".SelfPartitioner");
        producer = new KafkaProducer<>(properties);
        try {
            /*待发送的消息实例*/
            ProducerRecord<String, String> record;
            try {
                record = new ProducerRecord<>(
                    BusiConst.SELF_PARTITION_TOPIC, "teacher01",
                    "mark");
                Future<RecordMetadata> future = producer.send(record);
                System.out.println("Do other something");
                RecordMetadata recordMetadata = future.get();
                if (null != recordMetadata) {
                    System.out.println(String.format("偏移量：%s,分区：%s",
                        recordMetadata.offset(),
                        recordMetadata.partition()));
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        } finally {
            producer.close();
        }
    }
}
