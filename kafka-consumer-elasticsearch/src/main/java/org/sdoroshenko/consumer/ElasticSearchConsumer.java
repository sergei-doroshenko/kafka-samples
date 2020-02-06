package org.sdoroshenko.consumer;

import org.apache.http.HttpHost;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

/**
 * Assign and seek API example.
 */
public class ElasticSearchConsumer implements Runnable {
    private static final Logger logger = LoggerFactory.getLogger(ElasticSearchConsumer.class);
    private final RestHighLevelClient client;
    private final KafkaConsumer<String, String> consumer;
    private final CountDownLatch latch;

    public ElasticSearchConsumer(String bootstrapServers, String groupId, String topic, CountDownLatch latch) {
        this.client = createClient();
        this.consumer = createConsumer(bootstrapServers, groupId, topic);
        this.latch = latch;
    }

    private RestHighLevelClient createClient() {
        RestClientBuilder builder = RestClient.builder(new HttpHost("localhost", 9200, "http"));
        return new RestHighLevelClient(builder);
    }

    private KafkaConsumer<String, String> createConsumer(String bootstrapServers, String groupId, String topic) {
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);
        Collection<String> topics = Collections.singleton(topic);
        consumer.subscribe(topics);
        return consumer;
    }

    @Override
    public void run() {
        try {
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                for (ConsumerRecord<String, String> record : records) {
                    String jsonValue = record.value();
                    if (jsonValue != null) {
                        logger.info("Key:{}, value:{}", record.key(), record.value());

                        IndexRequest indexRequest = new IndexRequest("twitter")
                                .source(record.value(), XContentType.JSON);
                        IndexResponse indexResponse = client.index(indexRequest, RequestOptions.DEFAULT);
                        String id = indexResponse.getId();
                        logger.info("Document indexed, id: " + id);
                    }
                }
            }
        } catch (WakeupException e) {
            logger.info("Received shutdown signal!");
        } catch (IOException e) {
            logger.error("Error while indexing", e);
        } finally {
            consumer.close();
            try {
                client.close();
            } catch (IOException e) {
                logger.error("Error while closing client", e);
            }
            latch.countDown();
        }
    }

    public void shutdown() {
        consumer.wakeup();
    }
}
