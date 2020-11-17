package cn.enjoyedu.controller;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/**
 * kafka示例
 */
@RestController
@RequestMapping("/kafka")
public class KafkaController {
    protected final Logger logger = LoggerFactory.getLogger(this.getClass());
    @Autowired
    private KafkaTemplate kafkaTemplate;

    //自动确认
    @RequestMapping(value = "/send")
    public String sendKafka(@RequestParam(required = false) String key,
                            @RequestParam(required = false) String value) {
        try {
            logger.info("kafka的消息={}", value);
            kafkaTemplate.send("test", key, value);
            return "发送kafka成功";
        } catch (Exception e) {
            logger.error("发送kafka异常：", e);
            return "发送kafka失败";
        }
    }

    //手动确认
    @RequestMapping(value = "/sendAck")
    public String sendKafkaAck(@RequestParam(required = false) String key,
                            @RequestParam(required = false) String value) {
        try {
            logger.info("kafka的消息={}", value);
            kafkaTemplate.send("testAck", key, value);
            return "发送kafka成功";
        } catch (Exception e) {
            logger.error("发送kafka异常：", e);
            return "发送kafka失败";
        }
    }
}
