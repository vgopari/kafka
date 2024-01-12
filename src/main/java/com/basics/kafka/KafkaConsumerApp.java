package com.basics.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.CooperativeStickyAssignor;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.ApplicationListener;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.List;
import java.util.Properties;

@Component
public class KafkaConsumerApp implements ApplicationListener<ApplicationReadyEvent> {
     
    private static final Logger logger = LoggerFactory.getLogger(KafkaConsumerApp.class);
 
    public void onApplicationEvent(ApplicationReadyEvent event) {
        logger.info("Kafka Consumer Triggered");
        String groupId = "my-spring-app";

        //create consumer properties
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "127.0.0.1:9092");

        properties.setProperty("key.deserializer", StringDeserializer.class.getName());
        properties.setProperty("value.deserializer", StringDeserializer.class.getName());

        properties.setProperty("group.id", groupId);

        //possible values for auto.offset.reset: none/earliest/latest
        //none: if no consumer group is there then we will fail to consume
        //earliest: from beginning of topic
        //latest: only read new messages
        properties.setProperty("auto.offset.reset", "latest");

        properties.setProperty("partition.assignment.strategy", CooperativeStickyAssignor.class.getName());

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);

        // get a reference to a main thread
        final Thread mainThread = Thread.currentThread();

        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                logger.info("Detected a shutdown , let's exit by calling consume.wakeup()...");
                consumer.wakeup();

                //join the main thread to allow the execution of the code to join the main thread

                try {
                    mainThread.join();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        });

        try {
            //subscribe to a topic
            consumer.subscribe(List.of("first-topic"));

            //poll for data
            while (true) {
                ConsumerRecords<String, String> consumerRecords = consumer.poll(Duration.ofMillis(1000));
                for (ConsumerRecord<String, String> record: consumerRecords) {
                    logger.info("Key: " + record.key() +
                            " | Topic: " + record.topic() +
                            " | Partition: " + record.partition() +
                            " | Offset: " + record.offset() +
                            " | Timestamp: " + record.timestamp()
                    );
                }
            }
        } catch (WakeupException e) {
            logger.info("COnsumer is starting to shutdown");
            e.printStackTrace();
        } catch (Exception e) {
            logger.info("unexpected exception");
            e.printStackTrace();
        } finally {
            consumer.close();
            logger.info("Consumer is gracefully shutdown!");
        }

    }
}