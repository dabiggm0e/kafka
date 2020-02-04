package com.github.dabiggm0e.kafka.demo;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemoWithCallback {
    public static void main(String[] args) {
        Logger log = LoggerFactory.getLogger(ProducerDemoWithCallback.class);
        String bootstrap_servers = "localhost:9092";

        // create producer properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap_servers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // create producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        for(int i=0; i<10; i++)
            {
                // create producer record
                ProducerRecord<String, String> record = new ProducerRecord<>("first_topic", "Hello world from Java " + i);

                // send data - async

                producer.send(record, (recordMetadata, e) -> {
                    // if record was sent successfully
                    if (e == null) {
                        log.info("New record was produced with the following metadata:" + "\n"
                                + "Topic: " + recordMetadata.topic() + "\n"
                                + "Partition: " + recordMetadata.partition() + "\n"
                                + "Offset: " + recordMetadata.offset() + "\n"
                                + "Timestamp: " + recordMetadata.timestamp() + "\n"
                        );
                    }
                    //if there was an exception
                    else {
                        log.error("There was an exception while producing.", e);
                    }
                });
            }

        // flush data
        producer.flush();

        // close and flush data
    }


}
