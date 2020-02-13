package org.sdoroshenko.producer;

import com.google.common.collect.Lists;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class TwitterProducer implements Runnable {
    private static final Logger logger = LoggerFactory.getLogger(TwitterProducer.class);
    /**
     * Set up your blocking queues: Be sure to size these properly based on expected TPS of your stream
     */
    private final BlockingQueue<String> msgQueue = new LinkedBlockingQueue<>(1000);
    private final List<String> terms = Lists.newArrayList("bitcoin", "usa", "politics");
    private final Client twitterClient;
    private final String topic;
    private final KafkaProducer<String, String> producer;

    public TwitterProducer(String topic) {
        this.twitterClient = createTwitterClient(msgQueue, terms);
        this.topic = topic;
        this.producer = createKafkaProducer();
    }

    private Client createTwitterClient(BlockingQueue<String> msgQueue, List<String> terms) {

        Properties twitterAppProperties;
        try {
            twitterAppProperties = new PropertiesReader().read();
        } catch (IOException e) {
            logger.error("Error while reading Twitter App properties", e);
            throw new RuntimeException("Error while Twitter Client initialization", e);
        }

        /** Declare the host you want to connect to, the endpoint, and authentication (basic auth or oauth) */
        Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
        StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();
        hosebirdEndpoint.trackTerms(terms);

        Authentication hosebirdAuth = new OAuth1(
                twitterAppProperties.getProperty("consumerKey"),
                twitterAppProperties.getProperty("consumerSecret"),
                twitterAppProperties.getProperty("token"),
                twitterAppProperties.getProperty("secret")
        );

        ClientBuilder builder = new ClientBuilder()
                .name("Hosebird-Client-01")                              // optional: mainly for the logs
                .hosts(hosebirdHosts)
                .authentication(hosebirdAuth)
                .endpoint(hosebirdEndpoint)
                .processor(new StringDelimitedProcessor(msgQueue));

        return builder.build();
    }

    private KafkaProducer<String, String> createKafkaProducer() {
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        // create safe Producer
        properties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        properties.setProperty(ProducerConfig.ACKS_CONFIG, "all");
        properties.setProperty(ProducerConfig.RETRIES_CONFIG, Integer.toString(Integer.MAX_VALUE));
        // for Kafka less then 1.1 keep this as 5
        properties.setProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "5");

        // high throughput settings
        properties.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");
        properties.setProperty(ProducerConfig.LINGER_MS_CONFIG, "20");
        properties.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, Integer.toString(32 * 1024)); // 32 Kb batch size


        return new KafkaProducer<>(properties);
    }

    @Override
    public void run() {
        twitterClient.connect();

        while (!twitterClient.isDone()) {
            try {
                String msg = msgQueue.poll(5, TimeUnit.MILLISECONDS);
                if (msg != null) {
                    logger.info(msg);
                    String key = "id_" + 10; // can be omitted
                    ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, msg);
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
                }
            } catch (InterruptedException e) {
                logger.error("Twitter client have been interrupted", e);
                twitterClient.stop();
                Thread.currentThread().interrupt();
            }
        }
    }

    public void shutdown() {
        twitterClient.stop();
//        producer.flush();
        producer.close();
    }
}
