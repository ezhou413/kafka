/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.streams.examples.wordcount;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.ClientInstanceIds;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.GlobalKTable;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Produced;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Locale;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

/**
 * Demonstrates, using the high-level KStream DSL, how to implement the WordCount program
 * that computes a simple word occurrence histogram from an input text.
 * <p>
 * In this example, the input stream reads from a topic named "streams-plaintext-input", where the values of messages
 * represent lines of text; and the histogram output is written to topic "streams-wordcount-output" where each record
 * is an updated count of a single word.
 * <p>
 * Before running this example you must create the input topic and the output topic (e.g. via
 * {@code bin/kafka-topics.sh --create ...}), and write some data to the input topic (e.g. via
 * {@code bin/kafka-console-producer.sh}). Otherwise you won't see any data arriving in the output topic.
 */
public final class WordCountDemo {

    public static final String INPUT_TOPIC = "input";
    public static final String OUTPUT_TOPIC = "output";
    public static final String GLOBAL_TOPIC = "globalV3";
    private static final Logger LOG = LoggerFactory.getLogger(WordCountDemo.class);

    static Properties streamsConfig(final String[] args) throws IOException {
        final String path;
        if (args.length > 0) {
            path = args[0];
        } else {
            path = "streams/examples/src/main/java/org/apache/kafka/streams/examples/wordcount/streams-metrics.properties";
        }
        final Properties props = new Properties();
        if (path != null) {
            try (final FileInputStream fis = new FileInputStream(path)) {
                props.load(fis);
            }
            if (args.length > 1) {
                LOG.warn("Some command line arguments were ignored. This demo only accepts an optional configuration file.");
            }
        }
        props.putIfAbsent(StreamsConfig.APPLICATION_ID_CONFIG, "streams-wordcount");
        props.putIfAbsent(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.putIfAbsent(StreamsConfig.STATESTORE_CACHE_MAX_BYTES_CONFIG, 0);
        props.putIfAbsent(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);
        props.putIfAbsent(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);
        props.putIfAbsent(StreamsConfig.ENABLE_METRICS_PUSH_CONFIG, true);
        props.putIfAbsent(StreamsConfig.NUM_STREAM_THREADS_CONFIG, 3);
        props.putIfAbsent(StreamsConfig.topicPrefix("retention.ms"), 3600000);
        props.putIfAbsent(StreamsConfig.GROUP_PROTOCOL_CONFIG, "classic");

        // setting offset reset to earliest so that we can re-run the demo code with the same pre-loaded data
        // Note: To re-run the demo, you need to use the offset reset tool:
        // https://cwiki.apache.org/confluence/display/KAFKA/Kafka+Streams+Application+Reset+Tool
        props.putIfAbsent(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        return props;
    }

    static void createWordCountStream(final StreamsBuilder builder, final AtomicInteger inputCounter, final AtomicInteger outputCounter) {
        final KStream<String, String> source = builder.stream(INPUT_TOPIC);
        final GlobalKTable<String, String> globalTable = builder.globalTable(GLOBAL_TOPIC, Consumed.with(Serdes.String(), Serdes.String()));

        final KTable<String, Long> counts = source.peek((key, value) -> {
               inputCounter.incrementAndGet();
               if (inputCounter.get() % 10_000 == 0) {
                   LOG.info("{} Consumed {} records", new Date(), inputCounter.get());
               }
            })
                .flatMapValues(value -> Arrays.asList(value.toLowerCase(Locale.getDefault()).split("\\W+")))
                .groupBy((key, value) -> value)
                .count();

        // need to override value serde to Long type
        counts.toStream()
                .leftJoin(globalTable, (k, v) -> k, (v1, v2) -> v1 + 1)
                .peek((key, value) -> {
                    outputCounter.incrementAndGet();
                    if (outputCounter.get() % 10_000 == 0) {
                        LOG.info("{} Processed records: Count: {}", new Date(), outputCounter.get());
                    }
                })
                .to(OUTPUT_TOPIC, Produced.with(Serdes.String(), Serdes.Long()));
    }

    public static void main(final String[] args) throws IOException, ExecutionException, InterruptedException {
            final Properties props = streamsConfig(args);

            // List of 100 Kafka phrases
            final List<String> kafkaWords = Arrays.asList(
                    "Kafka connects the world", "Stream processing made easy", "Producers and Consumers in harmony",
                    "Zookeeper orchestrates", "Brokers handle the load", "Messages in partitions",
                    "Topic is the name of the game", "Exactly-once semantics", "At-least-once delivery",
                    "Apache Kafka native client", "Stream rebalance magic", "Offset management done right",
                    "Avro serialization rules", "JSON for simplicity", "Compact message format",
                    "Kafka Streams application", "Spring Kafka integration", "Microservices communicate better",
                    "Event-driven power", "Asynchronous messaging standard", "Cloud-native Kafka",
                    "Performance at scale", "Decoupling producers and consumers", "Distributed systems simplified",
                    "Partition leads to scalability", "Consumers in a group", "Highly available, fault-tolerant",
                    "Durable message log", "Cross-data center replication", "Kafka Connect integrations",
                    "Processing pipelines simple", "Unbounded streams in motion", "Real-time analytics support",
                    "KTables and streaming state", "RocksDB backend storage", "Stateful transformations",
                    "Functionally rich API", "Parallel processing capability", "Event streams for all",
                    "Data in transit simplified", "Streaming ETL made possible", "Kafka on Kubernetes",
                    "Offsets commit manually", "Consumer lag monitoring", "Kafka CLI tools manage all",
                    "Data pipeline resilience", "Low latency architecture", "Producer retries on failure",
                    "Throughput optimization science", "Kafka's internals demystified", "Repartition for balance",
                    "Real-world streaming use cases", "Core to modern architectures", "Kafka streams join tables",
                    "Error handling crucial", "Load testing in progress", "Scalability through sharding",
                    "Supercharging your microservices", "Exactly-once or bust!", "Kafka as event backbone",
                    "Oversimplified, yet complex", "Transactional guarantees exist", "Admin API useful",
                    "Kafka Summit knowledge shared", "Running in production", "Lenses over Kafka streams",
                    "Stream topology optimized", "Processing unbounded datasets", "Stateless streaming operations",
                    "Schema evolution handled", "Immutable logs forever", "Cloud-native deployments easy",
                    "Monitoring lag carefully", "Integration with Hadoop stack", "Kafka as event-first approach",
                    "Multi-cluster configurations possible", "Infinite scalability promise", "Zookeeper replacement debates",
                    "Concurrency model tested", "Machine learning on streams", "Event replay capability",
                    "Advanced fault tolerance", "Global KTables useful", "Kafka Streams DSL handy",
                    "Exploring KSQL DB magic", "Asynchronous evolution evident", "Lambda architecture support",
                    "Infinite scalability dreams", "Replication factor calculated", "Transactional producer benefits",
                    "Dynamic consumer groups grow", "Getting brokers balanced", "Stream-as-a-service integration",
                    "Prometheus metrics exposed", "APIs for extensibility", "Cleaner log mechanism",
                    "Kafka as pub-sub evolution", "Event mesh encompassing all"
            );

            final Thread producer = new Thread(() -> {
                props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, Serdes.String().serializer().getClass().getName());
                props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, Serdes.String().serializer().getClass().getName());
                AtomicInteger counter = new AtomicInteger(0);
                try (KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(props)) {

                    // Publish first 25 single words to global topic
                    kafkaWords.stream()
                            .map(phrase -> phrase.split("\\s+")[0])
                            .limit(10_000)
                            .forEach(word -> kafkaProducer.send(new ProducerRecord<>(GLOBAL_TOPIC, word, word),
                                    (metadata, exception) -> {
                                        if (exception != null) {
                                            LOG.error("Error while producing message to global topic {}: {}",
                                                    metadata.topic(), exception.getMessage());
                                        } else {
                                            counter.incrementAndGet();
                                            if (counter.get() % 5000 == 0) {
                                                LOG.info("Produced {} messages to global topic", counter.get());
                                            }
                                    }}));
                    LOG.info("{} Produced {} total messages to global topic", new Date(), counter.get());
                    counter.set(0);
                    while (true) {
                        kafkaWords.forEach(word -> kafkaProducer.send(new ProducerRecord<>(INPUT_TOPIC, null, word),
                                (metadata, exception) -> {
                                    if (exception != null) {
                                        LOG.error("Error while producing message to topic {}: {}", metadata.topic(), exception.getMessage());
                                    } else {
                                        counter.incrementAndGet();
                                        if (counter.get() % 10_000 == 0) {
                                            LOG.info("{} Produced {} messages to input topic", new Date(), counter.get());
                                        }
                                    }
                                }));
                        try {
                            Thread.sleep(250);
                        } catch (final InterruptedException e) {
                            // don't care
                        }
                    }
                }
            });

            producer.start();

            final StreamsBuilder builder = new StreamsBuilder();
            final AtomicInteger counter = new AtomicInteger(0);
            final AtomicInteger outputCounter = new AtomicInteger(0);
            createWordCountStream(builder, counter, outputCounter);
            final KafkaStreams streams = new KafkaStreams(builder.build(), props);
            final CountDownLatch latch = new CountDownLatch(1);

            // attach shutdown handler to catch control-c
            Runtime.getRuntime().addShutdownHook(new Thread("streams-wordcount-shutdown-hook") {
                @Override
                public void run() {
                    streams.close(Duration.ofSeconds(5));
                    latch.countDown();
                }
            });

            try {
                streams.cleanUp();
                streams.start();
                Thread.sleep(60_000L);
                ClientInstanceIds clientInstanceIds = streams.clientInstanceIds(Duration.ofSeconds(360));
                System.out.println("!! Admin client instance ids " + clientInstanceIds.adminInstanceId());
                System.out.printf("!! Consumer client instance ids %s%n", clientInstanceIds.consumerInstanceIds());
                System.out.printf("!! Producer client instance ids %s%n", clientInstanceIds.producerInstanceIds());
                latch.await();
            } catch (final Throwable e) {
                e.printStackTrace();
                System.out.printf("!!Error %s%n", e.getMessage());
                System.exit(1);
            }
            System.exit(0);
    }
}
