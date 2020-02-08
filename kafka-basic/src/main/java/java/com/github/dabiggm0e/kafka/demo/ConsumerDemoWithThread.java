package java.com.github.dabiggm0e.kafka.demo;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class ConsumerDemoWithThread {

    public static void main(String[] args) {

        new ConsumerDemoWithThread().run();

    }

    private   ConsumerDemoWithThread() {

    }

    private void run() {
        Logger logger = LoggerFactory.getLogger(ConsumerDemoWithThread.class.getName());

        String bootstrapServers = "localhost:9092";
        String groupId = "my-fifth-application";
        String topic = "first_topic";
        CountDownLatch latch = new CountDownLatch(1);

        logger.info("Creating the consumer thread");
        Runnable myConsumerRunnable = new ConsumerRunnable(
                bootstrapServers,
                groupId,
                topic,
                latch
        );

        Thread myThread = new Thread(myConsumerRunnable);
        myThread.start();

        // add shutdown hook
        Runtime.getRuntime().addShutdownHook( new Thread(
                () -> {
                    logger.info("Caught shutdown hook");
                    ((ConsumerRunnable) myConsumerRunnable).shutdown();
                }
        ));
        try {
            latch.await();
        } catch (InterruptedException e) {
            logger.info("Application got interrupted", e);
        }
        finally {
             logger.info("Application is closing");
        }
    }

    public class ConsumerRunnable implements Runnable {

        private CountDownLatch latch;
        private KafkaConsumer<String, String>  consumer;
        int duration = 100;
        Logger logger = LoggerFactory.getLogger(ConsumerRunnable.class.getName());


        public ConsumerRunnable(String bootstrapServers,
                              String groupId,
                              String topic,
                              CountDownLatch latch) {
            this.latch = latch;

            Properties properties = new Properties();

            properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
            properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
            properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);

            // create consumer
            consumer = new KafkaConsumer<>(properties);

            //subscribe to topic(s)
            consumer.subscribe(Arrays.asList(topic));



        }

        @Override
        public void run() {
            // poll data
            try {
                while (true) {
                    ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(duration));
                    for (ConsumerRecord<String, String> record: records) {
                        logger.info("Topic: " + record.topic() + ", Key: " + record.key() + ", Value: " + record.value());
                        logger.info("Partition: " + record.partition() + ", Offset: " + record.offset());
                    }
                }
            } catch (WakeupException e) {
                logger.info("Receievd shutdown signal!");
            }
            finally {
                 consumer.close();
                 // tell main code we are done with the consumer
                latch.countDown();
            }
        }

        public void shutdown() {
            // the wakeup() method is a special method to interrupt consumer.poll()
            // it will throw WakeUpException
            consumer.wakeup();
        }
    }
}
