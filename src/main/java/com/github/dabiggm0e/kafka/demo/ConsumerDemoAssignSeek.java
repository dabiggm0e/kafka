package com.github.dabiggm0e.kafka.demo;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ConsumerDemoAssignSeek {

    public static void main(String[] args) {

        String bootstrapServers = "localhost:9092";
        String groupId = "my-fifth-application";
        String topic = "first_topic";
        int partition = 0;
        int duration = 100;
        long offset = 15L;


        Logger logger = LoggerFactory.getLogger(ConsumerDemoAssignSeek.class.getName());
        Properties properties = new Properties();

        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        // create consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);

        // create topic partition
        TopicPartition topicPartition = new TopicPartition(topic, partition);

        //assign consumer to topic
        consumer.assign(Arrays.asList(topicPartition));

        // seek consumer
        consumer.seek(topicPartition, offset);

        int messagesToRead = 5;
        boolean doneReading = false;
        int messagesRead = 0;

        // poll data
        while (!doneReading) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(duration));

            for (ConsumerRecord<String, String> record: records) {
                ++messagesRead;
                logger.info("Topic: " + record.topic() + ", Key: " + record.key() + ", Value: " + record.value());
                logger.info("Partition: " + record.partition() + ", Offset: " + record.offset());

                if (messagesRead>=messagesToRead) {
                     doneReading = true;
                     break;
                }
            }


        }


    }
}
