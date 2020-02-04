package com.github.dabiggm0e.kafka.demo;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class ProducerDemo {
    public static void main(String[] args) {
        String bootstrap_servers = "localhost:9092";

        // create producer properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap_servers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // create producer

        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);
        // create producer record

        ProducerRecord<String, String> record = new ProducerRecord<>("first_topic", "Hello world from Java!");
        // send data

        producer.send(record);
        // flush data
        producer.flush();

        // close and flush data
    }


}
