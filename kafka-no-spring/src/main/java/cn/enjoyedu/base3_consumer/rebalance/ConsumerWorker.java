package cn.enjoyedu.base3_consumer.rebalance;

import cn.enjoyedu.config.BusiConst;
import cn.enjoyedu.config.KafkaConst;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class ConsumerWorker implements Runnable {

    private final KafkaConsumer<String, String> consumer;
    // 用来保存每个消费者当前读取分区的偏移量,解决分区再均衡的关键
    private final Map<TopicPartition, OffsetAndMetadata> currOffsets;
    private final boolean isStop;

    public ConsumerWorker(boolean isStop) {
        //消息消费者配置
        Properties properties = KafkaConst.consumerConfig(RebalanceConsumer.GROUP_ID, StringDeserializer.class, StringDeserializer.class);
        // 取消自动提交
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        this.isStop = isStop;
        this.consumer = new KafkaConsumer<>(properties);
        // 偏移量
        this.currOffsets = new HashMap<>();
        // 消费者订阅时加入再均衡监听器(HandlerRebalance)
        consumer.subscribe(Collections.singletonList(BusiConst.REBALANCE_TOPIC), new HandlerRebalance(currOffsets, consumer));
    }

    public void run() {
        final String id = Thread.currentThread().getId() + "";
        int count = 0;
        TopicPartition topicPartition;
        try {
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(500));
                for (ConsumerRecord<String, String> record : records) {
                    System.out.println("消费者"+ id + "|" + String.format("处理消息：主题：%s，分区：%d，偏移量：%d，key：%s，value：%s",
                            record.topic(), record.partition(), record.offset(), record.key(), record.value()));
                    topicPartition = new TopicPartition(record.topic(), record.partition());
                    // 消费者消费时把偏移量提交到统一HashMap
                    currOffsets.put(topicPartition, new OffsetAndMetadata(record.offset() + 1, "no"));
                    count++;
                    //执行业务sql
                }
                // 同时将业务和偏移量入库
                if (currOffsets.size() > 0) {
                    for (TopicPartition topicPartitionkey : currOffsets.keySet()) {
                        HandlerRebalance.partitionOffsetMap.put(topicPartitionkey, currOffsets.get(topicPartitionkey).offset());
                    }
                }
                // 如果stop参数为true,这个消费者消费到第5个时自动关闭
                if (isStop && count >= 5) {
                    System.out.println(id + "-将关闭，当前偏移量为：" + currOffsets);
                    consumer.commitSync();
                    break;
                }
                consumer.commitSync();
            }
        } finally {
            consumer.close();
        }
    }
}