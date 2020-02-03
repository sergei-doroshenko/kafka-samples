package org.sdoroshenko;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.stream.IntStream;

public class Producer0 implements Runnable {
    private static final Logger logger = LoggerFactory.getLogger(Producer0.class);
    private final  String topic;
    private final KafkaProducer<String, String> producer;

    public Producer0(String topic) {
        this.topic = topic;
        this.producer = initProducer();
    }

    private KafkaProducer<String, String> initProducer() {
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        return new KafkaProducer<>(properties);
    }

    @Override
    public void run() {
        IntStream.range(0, 10).forEach(i -> {
            String value = i + ". Hello, Sergei! [" + System.currentTimeMillis() + ']';
            String key = "id_" + i;
            logger.info("Key:{}", key);
            ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, value);
            producer.send(record, (metadata, e) -> {
                if (e != null) {
                    logger.error("Error: {}", e.getMessage(), e);
                } else {
                    logger.info(
                        "Received meta data.\n\tTopic:{}\n\tPartition:{}\n\tOffset:{}\n\tTimestamp:{}",
                        metadata.topic(),
                        metadata.partition(),
                        metadata.offset(),
                        metadata.timestamp()
                    );
                }
            });
        });


        producer.flush();
        producer.close();
    }
}
