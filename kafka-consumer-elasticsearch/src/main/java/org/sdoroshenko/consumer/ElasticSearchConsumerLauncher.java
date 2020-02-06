package org.sdoroshenko.consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

public class ElasticSearchConsumerLauncher {

    private static final Logger logger = LoggerFactory.getLogger(ElasticSearchConsumerLauncher.class);

    public static void main(String[] args) {
        String bootstrapServers = "127.0.0.1:9092";
        String groupId = "kafka-demo-elasticsearch";
        String topic = "twitter_tweets";
        CountDownLatch latch = new CountDownLatch(1);
        Executor consumerExecutor = Executors.newSingleThreadExecutor();
        ElasticSearchConsumer consumer = new ElasticSearchConsumer(bootstrapServers, groupId, topic, latch);
        consumerExecutor.execute(consumer);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Caught shutdown hook!");
            consumer.shutdown();
            try {
                latch.await();
            } catch (InterruptedException e) {
                logger.error("Application interrupted!", e);
                Thread.currentThread().interrupt();
            }
        }));

        try {
            latch.await();
        } catch (InterruptedException e) {
            logger.error("Application interrupted!", e);
            Thread.currentThread().interrupt();
        } finally {
            logger.info("Application is closing.");
        }
    }
}
