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

    /*模拟一个保存分区偏移量的数据库表*/
    public final static ConcurrentHashMap<TopicPartition, Long> partitionOffsetMap = new ConcurrentHashMap<>();

    private final Map<TopicPartition, OffsetAndMetadata> currOffsets;
    private final KafkaConsumer<String, String> consumer;
    //private final Transaction  tr事务类的实例

    public HandlerRebalance(Map<TopicPartition, OffsetAndMetadata> currOffsets, KafkaConsumer<String, String> consumer) {
        this.currOffsets = currOffsets;
        this.consumer = consumer;
    }

    // 再均衡开始之前和消费者停止读取消息之后被调用。如果在这里提交偏移量，下一个接管分区的消费者就知道该从哪里开始读取了
    public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
        final String id = Thread.currentThread().getId() + "";
        System.out.println(id + "-onPartitionsRevoked参数值为：" + partitions);
        System.out.println(id + "-服务器准备分区再均衡，提交偏移量。当前偏移量为：" + currOffsets);
        //我们可以不使用consumer.commitSync(currOffsets);
        //提交偏移量到kafka,由我们自己维护*/
        //开始事务
        //偏移量写入数据库
        System.out.println(id + "分区偏移量表中：" + partitionOffsetMap);
        if(currOffsets.size()>0){
            for (TopicPartition topicPartition : partitions) {
                System.out.println(id + "保存分区偏移量：分区->" + topicPartition + "偏移量->" + currOffsets.get(topicPartition).offset());
                partitionOffsetMap.put(topicPartition, currOffsets.get(topicPartition).offset());
            }
        }
        consumer.commitSync(currOffsets);
        //提交业务数和偏移量入库  tr.commit
    }

    // 重新分配分区之后和消费者开始读取消息之前被调用
    public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
        final String id = Thread.currentThread().getId() + "";
        System.out.println(id + "-再均衡完成，onPartitionsAssigned参数值为：" + partitions);
        System.out.println(id + "分区偏移量表中：" + partitionOffsetMap);
        if (partitionOffsetMap.size() > 0) {
            for (TopicPartition topicPartition : partitions) {
                System.out.println(id + "-topicPartition" + topicPartition);
                //模拟从数据库中取得上次的偏移量
                Long offset = partitionOffsetMap.get(topicPartition);
                if (offset == null) continue;
                // 从特定偏移量处开始记录 (从指定分区中的指定偏移量开始消费)
                // 这样就可以确保分区再均衡中的数据不错乱
                consumer.seek(topicPartition, partitionOffsetMap.get(topicPartition));
            }
        }
    }
}