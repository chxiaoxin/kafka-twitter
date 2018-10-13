package tutorial1;

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

public class ConsumerThreadDemo {

    public ConsumerThreadDemo(){

    }

    private void runThread() {
        Logger logger = LoggerFactory.getLogger(ConsumerThreadDemo.class);
        String topicName = "demo_topic";
        String bootstrapServer = "localhost:9092";
        String serilizer = StringDeserializer.class.getName();
        String groupId = "thread_groups";
        String set = "earliest";
        CountDownLatch latch = new CountDownLatch(1);
        consumerThread threadConsumer = new consumerThread(topicName, bootstrapServer, serilizer, serilizer, groupId, set, latch);
        Thread thread = new Thread(threadConsumer);
        thread.start();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Caught shutdown info");
            threadConsumer.shutdown();
            try{
                threadConsumer.latch.await();
            } catch (InterruptedException e){
                e.printStackTrace();
            }finally{
                logger.info("Thread closed");
            }
        }));
    }

    public class consumerThread implements Runnable {

        private CountDownLatch latch;
        private KafkaConsumer<String, String> consumer;
        private Properties properties = new Properties();
        private Logger logger = LoggerFactory.getLogger(ConsumerThreadDemo.class);

        public consumerThread(String topic,
                               String bootstrapServer,
                               String keyDesr,
                               String valueDesr,
                               String groupId,
                               String resetManner,
                               CountDownLatch latch
                               )
        {
            properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
            properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, keyDesr);
            properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, valueDesr);
            properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
            properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, resetManner);
            this.latch = latch;
            consumer = new KafkaConsumer<String, String>(properties);
            consumer.subscribe(Arrays.asList(topic));

        }
        @Override
        public void run() {
            try {
                while(true) {
                    ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                    for (ConsumerRecord<String, String> record : records) {
                        logger.info("key is :" + record.key());
                        logger.info("value is :" + record.value());
                    }
                }
            }
            catch(WakeupException e) {
                logger.info("Received shutdown signal");

            }finally{
                logger.info("Consumer closed");
                consumer.close();
            }
            latch.countDown();
        }

        public void shutdown() {
            consumer.wakeup();
        }
    }

    public static void main(String[] args) {
        ConsumerThreadDemo demoThread = new ConsumerThreadDemo();
        demoThread.runThread();
    }
}
