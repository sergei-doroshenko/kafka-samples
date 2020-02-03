package org.sdroshenko;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

public class ConsumerLauncher0 {

    private static final Logger logger = LoggerFactory.getLogger(ConsumerLauncher0.class);

    public static void main(String[] args) {
        String bootstrapServers = "127.0.0.1:9092";
        String groupId = "my-first-application";
        String topic = "first_topic";
        CountDownLatch latch = new CountDownLatch(1);
        Executor consumerExecutor = Executors.newFixedThreadPool(2);
        Consumer0 consumer = new Consumer0(bootstrapServers, groupId, topic, latch);
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
