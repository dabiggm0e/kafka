package com.github.dabiggm0e.kafka.demo.twitter;

import com.google.common.collect.Lists;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.*;

public class TwitterProducer {

    String CONSUMER_KEY;
    String CONSUMER_SECRET;
    String ACCESS_TOKEN;
    String ACCESS_TOKEN_SECRET;

    Logger logger =  LoggerFactory.getLogger(TwitterProducer.class.getName());
    Properties prop;

    String topic = "twitter-tweets";
    String key = "Sudan";
    String bootstrap_servers = "localhost:9092";

    ArrayList<String> twitterStreamingTerms = Lists.newArrayList("Sudan", "USA", "Politics");

    public TwitterProducer(String CONSUMER_KEY, String CONSUMER_SECRET, String ACCESS_TOKEN, String ACCESS_TOKEN_SECRET) {
        this.CONSUMER_KEY = CONSUMER_KEY;
        this.CONSUMER_SECRET = CONSUMER_SECRET;
        this.ACCESS_TOKEN = ACCESS_TOKEN;
        this.ACCESS_TOKEN_SECRET = ACCESS_TOKEN_SECRET;

    }
    public TwitterProducer(Properties prop) {
        this.prop = prop;
        this.CONSUMER_KEY =  prop.getProperty("CONSUMER_KEY");
        this.CONSUMER_SECRET =  prop.getProperty("CONSUMER_SECRET");
        this.ACCESS_TOKEN =  prop.getProperty("ACCESS_TOKEN");
        this.ACCESS_TOKEN_SECRET =  prop.getProperty("ACCESS_TOKEN_SECRET");

    }

    public static void main(String[] args)  {

        InputStream input = TwitterProducer.class.getClassLoader().getResourceAsStream("twitter.properties");
        Properties prop = new Properties();

        if (input == null) {
            System.out.println("Sorry, unable to find config.properties");
        }
        try {
            prop.load(input);
        } catch (IOException e) {
            e.printStackTrace();
        }

        String CONSUMER_KEY =  prop.getProperty("CONSUMER_KEY");
        String CONSUMER_SECRET =  prop.getProperty("CONSUMER_SECRET");
        String ACCESS_TOKEN =  prop.getProperty("ACCESS_TOKEN");
        String ACCESS_TOKEN_SECRET =  prop.getProperty("ACCESS_TOKEN_SECRET");

        new TwitterProducer(
                prop
        ).run();
    }

    public void run() {
        //create twitter client
        /** Set up your blocking queues: Be sure to size these properly based on expected TPS of your stream */
        BlockingQueue<String> msgQueue = new LinkedBlockingQueue<String>(100000);

        Client client = createTwitterClient(msgQueue);
        client.connect();

        // create kafka producer
        KafkaProducer<String, String> producer = createKafkaProducer();

        // add a shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(()-> {
            logger.info("Shutting down application...");
            logger.info("Stopping client...");
            client.stop();
            logger.info("Stopping producer...");
            producer.close();
            logger.info("done...");
        }));

        // loop
        while (!client.isDone()) {
            String msg = null;
            try {
                msg = msgQueue.poll(5, TimeUnit.SECONDS);
                System.out.println(msg);
            } catch (InterruptedException e) {
                e.printStackTrace();
                client.stop();
            }

            if(msg != null) {
                ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topic, key, msg);
                logger.info(msg);
                producer.send(producerRecord, new Callback() {

                    @Override
                    public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                        if (e!= null) {
                            logger.info("Something bad happened", e);
                        }
                    }
                });
                producer.flush();
            }
        }

        logger.info("End of application");
    }

    private KafkaProducer<String, String> createKafkaProducer() {

        prop.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap_servers);
        prop.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        prop.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // create a safe producer
        prop.setProperty(ProducerConfig.ACKS_CONFIG, "all");
        prop.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, Integer.toString(32*1024));
        prop.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        prop.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");
        prop.setProperty(ProducerConfig.LINGER_MS_CONFIG, "20");
        prop.setProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "5");
        prop.setProperty(ProducerConfig.RETRIES_CONFIG, Integer.toString(Integer.MAX_VALUE));




        KafkaProducer<String, String>  producer = new KafkaProducer<String, String>(prop);
        return producer;
    }

    public Client createTwitterClient(BlockingQueue<String> msgQueue ) {


/** Declare the host you want to connect to, the endpoint, and authentication (basic auth or oauth) */
        Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
        StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();
// Optional: set up some followings and track terms
        List<Long> followings = Lists.newArrayList(1234L, 566788L);
        List<String> terms = Lists.newArrayList(twitterStreamingTerms);
        hosebirdEndpoint.followings(followings);
        hosebirdEndpoint.trackTerms(terms);


// These secrets should be read from a config file
        Authentication hosebirdAuth = new OAuth1(
                CONSUMER_KEY,
                CONSUMER_SECRET,
                ACCESS_TOKEN,
                ACCESS_TOKEN_SECRET
        );


        ClientBuilder builder = new ClientBuilder()
                .name("Hosebird-Client-01")                              // optional: mainly for the logs
                .hosts(hosebirdHosts)
                .authentication(hosebirdAuth)
                .endpoint(hosebirdEndpoint)
                .processor(new StringDelimitedProcessor(msgQueue));

        Client hosebirdClient = builder.build();
        return hosebirdClient;



    }

}
