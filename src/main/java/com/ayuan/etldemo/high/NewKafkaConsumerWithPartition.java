package com.ayuan.etldemo.high;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

import java.util.Collections;
import java.util.Properties;

public class NewKafkaConsumerWithPartition extends Thread {
    private String topic;
    private String bootstrapServer = "39.100.27.193:9092";
    private KafkaConsumer<Integer, String> kafkaConsumer;
    public NewKafkaConsumerWithPartition(String topic) {
        this.topic = topic;
        Properties props = new Properties();
        props.put("bootstrap.servers", bootstrapServer);
        //消费者组ID
        props.put("group.id", "test11");
        //设置自动提交offset
        props.put("enable.auto.commit", "true");
        //设置自动提交offset的延时（可能会造成重复消费的情况）
        props.put("auto.commit.interval.ms", "1000");
        //key-value的反序列化
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        kafkaConsumer = new KafkaConsumer<Integer, String>(props);
    }

    @Override
    public void run() {
        TopicPartition topicPartition = new TopicPartition(topic, 0);
        //消费指定分区
        kafkaConsumer.assign(Collections.singleton(topicPartition));
        //指定偏移量
        kafkaConsumer.seek(topicPartition, 3985);
        while (true) {
            //间隔100毫秒，从topic拉取消息
            ConsumerRecords<Integer, String> records = kafkaConsumer.poll(100);
            for (ConsumerRecord record : records) {
                System.out.println("==>  " + record.value());
                System.out.println("partition ==> " + record.partition());
                System.out.println("offset ==> " + record.offset());
            }
        }
    }
}
