package com.ayuan.etldemo.high;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Properties;

public class NewKafkaProducer extends Thread {
    private String topic;
    private String bootstrapServer = "39.100.27.193:9092";

    private KafkaProducer<Integer, String> producer;

    public NewKafkaProducer(String topic) {
        this.topic = topic;

        Properties prop = new Properties();

        prop.put("bootstrap.servers", bootstrapServer);
        prop.put("acks", "1");
        // key序列化
        prop.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        // value序列化
        prop.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        producer = new KafkaProducer<>(prop);
    }

    @Override
    public void run() {
        int i = 1;
        while (true) {
            ProducerRecord producerRecord = new ProducerRecord<Integer, String>(
                    "test",
                    "new_kafkaproducer ==> " + i);
            producer.send(producerRecord, new Callback() {
                @Override
                public void onCompletion(RecordMetadata metadata, Exception exception) {
                    if (exception != null) {
                        System.out.println("发送失败");
                        //数据补偿处理
                    } else {
                        System.out.println("offset:" + metadata.offset());
                        System.out.println("partition:" + metadata.partition());
                    }
                }
            });
            i++;
        }
    }
}
