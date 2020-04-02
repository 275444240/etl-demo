package com.ayuan.etldemo.controller;

import org.apache.kafka.clients.producer.Producer;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Properties;


@RestController
@RequestMapping("/high")
public class KafkaHighApiController {

    public String topic = "test";

}
