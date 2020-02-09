import com.google.gson.JsonParser;
import com.google.gson.JsonSyntaxException;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;

import java.util.Properties;
import java.util.logging.Logger;

public class StreamsFilterTweets {

    Logger logger = Logger.getLogger(StreamsFilterTweets.class.getName());
    static JsonParser jsonParser = new JsonParser();


    public static Integer extractFollowersFromTweet(String jsonRequest) {
        Logger logger = Logger.getLogger(StreamsFilterTweets.class.getName());

        Integer followers = 0;

        try {
             followers = jsonParser.parse(jsonRequest)
                    .getAsJsonObject()
                     .get("payload")
                     .getAsJsonObject()
                     .get("User")
                     .getAsJsonObject()
                     .get("FollowersCount")
                     .getAsInt();

            logger.info("followers: " + followers);
        } catch (NullPointerException e) {
            logger.info("Bad data: " + jsonRequest);
          return 0;
        }


        return followers;
    }

    public static void main(String[] args) {

        String bootstrapServer = "localhost:9092";
        String appId = "kafka-streams";
        String topic = "twitter-status-connect";

        // create properties
        Properties properties = new Properties();
        properties.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        properties.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, appId);
        properties.setProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());
        properties.setProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());


        //create topology
        StreamsBuilder streamsBuilder = new StreamsBuilder();

        //input topic
        KStream<String, String> inputTopic = streamsBuilder.stream(topic);
        KStream<String, String> filteredStream = inputTopic.filter(
                (k, jsonTweets) ->
                        // filter for tweets with user having more than 10K followers
                        extractFollowersFromTweet(jsonTweets) > 10000
        );

        filteredStream.to("important-tweets");

        // build the topology
        KafkaStreams kafkaStreams = new KafkaStreams(
                streamsBuilder.build(),
                properties);

        //start the streams application
        kafkaStreams.start();
    }
}
