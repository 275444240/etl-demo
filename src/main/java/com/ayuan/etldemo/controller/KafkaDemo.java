package com.ayuan.etldemo.controller;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/kafka")
public class KafkaDemo {
    protected final Logger logger = LoggerFactory.getLogger(this.getClass());


    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;


    @RequestMapping(value = "/send", method = RequestMethod.GET)
    public String sendKafka(@RequestParam("message") String message) {
        try {
            logger.info("kafka的消息={}", message);
            kafkaTemplate.send("test", "key" + message.charAt(1), message);
            logger.info("发送kafka成功.");
            return "successs";
        } catch (Exception e) {
            logger.error("发送kafka失败", e);
            return "failure";
        }
    }


    @KafkaListener(topics = "test")
    public void listen(ConsumerRecord<?, ?> record) throws Exception {
        System.out.printf("++++++++++++++++++++++++");
        System.out.printf("topic = %s, offset = %s, value = %s \n", record.topic(), record.key(), record.value());
        System.out.printf("++++++++++++++++++++++++");
    }



}
