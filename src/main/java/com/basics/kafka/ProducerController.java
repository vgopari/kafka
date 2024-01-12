package com.basics.kafka;

import org.apache.kafka.clients.producer.*;
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
        ProducerRecord<String, String> producerRecord = new ProducerRecord<>("first-topic", "Hello!");


        //send data to kafka topic
        producer.send(producerRecord);

        //flush - tell the producer to send all the data and blocks untill done -- synchronous;
        producer.flush();

        //close the producer
        producer.close();
    }

    @GetMapping("/producer/callback")
    public void producerWithCallback() {
        //create producer properties
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "127.0.0.1:9092");

        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", StringSerializer.class.getName());
        properties.setProperty("batch.size", "400"); //to demonstrate the behaviour of sticky partitioner

//        properties.setProperty("partitioner.class", RoundRobinPartitioner.class.getName());

        //create producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        for (int j=0; j<10; j++) {

            for (int i = 0; i < 10; i++) {

                //sticky partitions:
                //create a producer record
                ProducerRecord<String, String> producerRecord = new ProducerRecord<>("first-topic", "Hello!"+i);


                //send data to kafka topic
                producer.send(producerRecord, new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                        if(e==null) {
                            // if exception is null then the record was sent successfully to kafka topic
//                            log.info("Received new Metadata: \n" +
//                                    "Topic: " + recordMetadata.topic() + "\n" +
//                                    "Partition: " + recordMetadata.partition() + "\n" +
//                                    "Offset: " + recordMetadata.offset() + "\n" +
//                                    "Timestamp: " + recordMetadata.timestamp() + "\n"
//                            );
                        } else {
                            log.error("Error while producing message", e);
                        }
                    }
                });
            }
            try {
                Thread.sleep(500);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }



        //flush - tell the producer to send all the data and blocks untill done -- synchronous;
        producer.flush();

        //close the producer
        producer.close();
    }


    @GetMapping("/producer/key")
    public void producerWIthKeys() {

        //create producer properties
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "127.0.0.1:9092");

        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", StringSerializer.class.getName());

        //create producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        for (int j = 0; j < 2; j++) {

            for (int i = 0; i < 10; i++) {

                String key = "key-"+i;

                //sticky partitions:
                //create a producer record
                ProducerRecord<String, String> producerRecord = new ProducerRecord<>("first-topic", key, "Hello-" + i);


                //send data to kafka topic
                producer.send(producerRecord, new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                        if (e == null) {
                            // if exception is null then the record was sent successfully to kafka topic
                            log.info("Key: " + key +
                                    " | Topic: " + recordMetadata.topic() +
                                    " | Partition: " + recordMetadata.partition() +
                                    " | Offset: " + recordMetadata.offset() +
                                    " | Timestamp: " + recordMetadata.timestamp()
                            );
                        } else {
                            log.error("Error while producing message", e);
                        }
                    }
                });
            }
            log.info("end of batch - " + j);
            try {
                Thread.sleep(500);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
        //flush - tell the producer to send all the data and blocks untill done -- synchronous;
        producer.flush();

        //close the producer
        producer.close();
    }
}
