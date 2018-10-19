import com.google.gson.JsonParser;
import org.apache.kafka.common.protocol.types.Field;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class kafkaStreamTwitter {
    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.setProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());
        properties.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "kafka-stream-demo");
        properties.setProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());

        StreamsBuilder stream = new StreamsBuilder();

        KStream<String, String> topicStream = stream.stream("twitter_topic");
        KStream<String, String> filteredStream = topicStream.filter(
                (k, v) -> extractCount(v) > 1000
        );
        filteredStream.to("important_tweets");

        KafkaStreams finalStream = new KafkaStreams(stream.build(), properties);

        finalStream.start();
    }

    private static Integer extractCount (String tweets) {
        Logger logger = LoggerFactory.getLogger(kafkaStreamTwitter.class.getName());
        JsonParser parser = new JsonParser();
        try{
            return parser.parse(tweets)
                    .getAsJsonObject()
                    .get("user")
                    .getAsJsonObject()
                    .get("followers_count")
                    .getAsInt();
        } catch(NullPointerException e){
            logger.info("bad data:" + tweets);
            return 0;
        }

    }
}
