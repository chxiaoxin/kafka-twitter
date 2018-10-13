package tutorial1;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemo {
    public static void main(String[] args) {

        Properties properties = new Properties();
        final Logger logger = LoggerFactory.getLogger(ProducerDemo.class);

        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        final ProducerRecord<String, String> record = new ProducerRecord<String, String>("demo_topic", "hello world");

        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

        producer.send(record, new Callback() {
            public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                if(e == null){
                    logger.info("partition is:" + recordMetadata.partition());
                    logger.info("send time is:" + recordMetadata.timestamp());
                }else{
                    logger.error("error while producing:", e);
                }
            }
        });

        producer.flush();
        producer.close();

    }
}
