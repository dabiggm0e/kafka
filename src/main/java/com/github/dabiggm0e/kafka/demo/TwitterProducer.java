package com.github.dabiggm0e.kafka.demo;

import com.github.dabiggm0e.twitter.TwitterClient;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import twitter4j.Status;
import twitter4j.Twitter;
import twitter4j.TwitterException;

import java.util.Collection;
import java.util.Properties;

public class TwitterProducer {
    public static void main(String[] args) throws TwitterException {
        TwitterClient twitterClient;

        Logger log = LoggerFactory.getLogger(TwitterProducer.class);
        String bootstrap_servers = "localhost:9092";
        String topic = "twitter-timeline";

        // create producer properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap_servers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // create producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        twitterClient = new TwitterClient();
        Collection<Status> statuses = twitterClient.getHomeTimeline();

        for(Status status: statuses) {

            String key = "id_" + status.getUser().getScreenName();
            String value = twitterClient.getStatusLine(status);

            // create producer record
            ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, value);

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
