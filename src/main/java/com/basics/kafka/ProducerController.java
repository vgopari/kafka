package com.basics.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Properties;

@RestController
public class ProducerController {

    private static final Logger log = LoggerFactory.getLogger(ProducerController.class.getSimpleName());

    @GetMapping("/producer")
    public void producer() {

        //create producer properties
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "127.0.0.1:9092");

        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", StringSerializer.class.getName());

        //create producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        //create a producer record
        ProducerRecord<String, String> producerRecord = new ProducerRecord<>("first-topic", "key1", "Hello!");


        //send data to kafka topic
        producer.send(producerRecord);

        //flush - tell the producer to send all the data and blocks untill done -- synchronous;
        producer.flush();

        //close the producer
        producer.close();
    }
}
