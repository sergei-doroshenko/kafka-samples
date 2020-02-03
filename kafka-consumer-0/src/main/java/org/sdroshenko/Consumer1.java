package org.sdroshenko;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

/**
 * Assign and seek API example.
 */
public class Consumer1 implements Runnable {
    private static final Logger logger = LoggerFactory.getLogger(Consumer1.class);
    private final KafkaConsumer<String, String> consumer;
    private final CountDownLatch latch;

    public Consumer1(String bootstrapServers, String topic, CountDownLatch latch) {
        this.consumer = initConsumer(bootstrapServers, topic);
        this.latch = latch;
    }

    private KafkaConsumer<String, String> initConsumer(String bootstrapServers, String topic) {

        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        // assign
        KafkaConsumer<String, String> aConsumer = new KafkaConsumer<>(properties);
        TopicPartition topicPartition = new TopicPartition(topic, 0);
        aConsumer.assign(Collections.singleton(topicPartition));

        // seek
        long offset = 15L;
        aConsumer.seek(topicPartition, offset);

        return aConsumer;
    }

    @Override
    public void run() {
        try {
            int numberOfMessagesToRead = 5;
            while (numberOfMessagesToRead > 0) {
                logger.info("Messages to read:{}", numberOfMessagesToRead);
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                for (ConsumerRecord<String, String> record : records) {
                    logger.info("Key:{}, value:{}", record.key(), record.value());
                    logger.info("Partition:{}, Offset:{}", record.partition(), record.offset());
                    if (--numberOfMessagesToRead <= 0) break;
                }
            }
        } catch (WakeupException e) {
            logger.info("Received shutdown signal!");
        } finally {
            consumer.close();
            latch.countDown();
        }
    }

    public void shutdown() {
        consumer.wakeup();
    }
}
