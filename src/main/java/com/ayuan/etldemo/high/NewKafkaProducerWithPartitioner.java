package com.ayuan.etldemo.high;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.logging.log4j.util.PropertiesUtil;

import java.util.Properties;

public class NewKafkaProducerWithPartitioner extends Thread {
    private String topic;
    private String bootstrapServer = "39.100.27.193:9092";

    private KafkaProducer<Integer, String> producer;

    public NewKafkaProducerWithPartitioner(String topic) {
        this.topic = topic;
        Properties prop = new Properties();
        prop.put("bootstrap.servers", bootstrapServer);
        prop.put("acks", "1");
        // key序列化
        prop.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        // value序列化
        prop.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        //指定自定义分区
        prop.put("partitioner.class", "com.monk.kafka.CustomPartitioner");
        producer = new KafkaProducer<>(prop);
    }

    @Override
    public void run() {

        for (int i = 1; i <= 10; i++) {

            ProducerRecord<Integer, String> producerRecord =
                    new ProducerRecord<>(topic, "NewKafkaProducerWithPartitioner ==> " + i);

            producer.send(producerRecord, (metadata, exception) -> {
                if (exception != null) {
                    System.out.println("发送失败");
                } else {
                    System.out.println("offset:" + metadata.offset());
                    System.out.println("partition:" + metadata.partition());
                }
            });
        }

        producer.close();
    }
}
