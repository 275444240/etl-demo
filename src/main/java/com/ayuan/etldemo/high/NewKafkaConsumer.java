package com.ayuan.etldemo.high;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Collections;
import java.util.Properties;

public class NewKafkaConsumer extends Thread {
    private String topic;
    private String bootstrapServer = "39.100.27.193:9092";
    private KafkaConsumer<Integer, String> kafkaConsumer;

    public NewKafkaConsumer(String topic) {
        this.topic = topic;
        Properties props = new Properties();
        props.put("bootstrap.servers", bootstrapServer);
        //消费者组ID
        props.put("group.id", "test1");
        //设置自动提交offset
        props.put("enable.auto.commit", "true");
        //设置自动提交offset的延时（可能会造成重复消费的情况）
        props.put("auto.commit.interval.ms", "1000");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        //key-value的反序列化
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        kafkaConsumer = new KafkaConsumer<Integer, String>(props);
    }


    @Override
    public void run() {
        kafkaConsumer.subscribe(Collections.singletonList(topic));
        while (true) {
            //间隔100毫秒，从topic拉取消息
            ConsumerRecords<Integer, String> records = kafkaConsumer.poll(100);

            for (ConsumerRecord record : records) {
                System.out.println("==>  " + record.value());
            }
        }
    }

}
