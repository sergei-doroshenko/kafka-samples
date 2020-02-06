package org.sdroshenko.producer;

import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

public class ProducerApp0 {
    public static void main(String[] args) {
        String topic = "first_topic";
        Executor producerExecutor = Executors.newSingleThreadExecutor();
        producerExecutor.execute(new Producer0(topic));
    }
}
