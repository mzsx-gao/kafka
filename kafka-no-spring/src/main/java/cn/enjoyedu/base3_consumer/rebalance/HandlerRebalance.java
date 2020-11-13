package cn.enjoyedu.base3_consumer.rebalance;

import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 再均衡监听器
 */
public class HandlerRebalance implements ConsumerRebalanceListener {

    public final static ConcurrentHashMap<TopicPartition, Long> partitionOffsetMap = new ConcurrentHashMap<>();

    private final Map<TopicPartition, OffsetAndMetadata> currOffsets;
    private final KafkaConsumer<String, String> consumer;

    //模拟一个保存分区偏移量的数据库表
    public HandlerRebalance(Map<TopicPartition, OffsetAndMetadata> currOffsets, KafkaConsumer<String, String> consumer) {
        this.currOffsets = currOffsets;
        this.consumer = consumer;
    }

    // 再均衡开始之前和消费者停止读取消息之后被调用。如果在这里提交偏移量，下一个接管分区的消费者就知道该从哪里开始读取了
    public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
        final String id = Thread.currentThread().getId() + "";
        System.out.println("再均衡开始之前，消费者[" + id + "]调用onPartitionsRevoked参数值为：" + partitions +
                ",当前消费偏移量currOffsets为:" + currOffsets);
        if (currOffsets.size() > 0) {
            for (TopicPartition topicPartition : partitions) {
                System.out.println(id + "保存分区偏移量：分区->" + topicPartition + "偏移量->" + currOffsets.get(topicPartition).offset());
                partitionOffsetMap.put(topicPartition, currOffsets.get(topicPartition).offset());
            }
            consumer.commitSync(currOffsets);
        }
    }

    // 重新分配分区之后和消费者开始读取消息之前被调用
    public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
        final String id = Thread.currentThread().getId() + "";
        System.out.println("消费者" + id + "再均衡完成之后,调用onPartitionsAssigned，参数值为:" + partitions +
                "，当前数据库分区偏移量表中:" + partitionOffsetMap);
        if (partitionOffsetMap.size() > 0) {
            for (TopicPartition topicPartition : partitions) {
                //模拟从数据库中取得上次的偏移量
                Long offset = partitionOffsetMap.get(topicPartition);
                if (offset == null) continue;
                System.out.println("消费者[" + id + "]从分区:" + topicPartition + ",偏移量：" + offset + "开始消费");
                // 从特定偏移量处开始记录 (从指定分区中的指定偏移量开始消费)，这样就可以确保分区再均衡中的数据不错乱
                consumer.seek(topicPartition, offset);
            }
        }
    }
}