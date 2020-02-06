package org.sdoroshenko.producer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

public class TwitterProducerApp {
    private static final Logger logger = LoggerFactory.getLogger(TwitterProducerApp.class);

    public static void main(String[] args) {
        String topic = "twitter_tweets";
        Executor producerExecutor = Executors.newSingleThreadExecutor();
        TwitterProducer producer = new TwitterProducer(topic);
        producerExecutor.execute(producer);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Caught shutdown hook!");
            producer.shutdown();
        }));
    }
}
