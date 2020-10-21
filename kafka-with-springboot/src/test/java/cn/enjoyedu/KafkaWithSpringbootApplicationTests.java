package cn.enjoyedu;

import cn.enjoyedu.controller.KafkaController;
import cn.enjoyedu.service.KafkaSender;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

@RunWith(SpringRunner.class)
@SpringBootTest(classes = KafkaWithSpringbootApplication.class)
public class KafkaWithSpringbootApplicationTests {

    @Autowired
    private KafkaController kafkaController;

    @Test
    public void test() throws Exception{
        kafkaController.sendKafka("test1","value1");
        Thread.sleep(1000);
//        kafkaController.sendKafkaAck("test2","value2");
        Thread.sleep(Integer.MAX_VALUE);
    }

}
