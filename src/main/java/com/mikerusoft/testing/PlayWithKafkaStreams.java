package com.mikerusoft.testing;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;

import java.nio.ByteBuffer;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.LinkedList;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class PlayWithKafkaStreams {

    public static final int WINDOW_DURATION_SEC = 60;

    public static void main(final String[] args) throws Exception {

        Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "stam-application");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "kafka-1:9092");
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        StreamsBuilder streamsBuilder = new StreamsBuilder();
        streamsBuilder.stream("stream-test0", Consumed.<String, String>with((record, previousTimestamp) -> extractMyTimeFrom(record)))
            .map((KeyValueMapper<String, String, KeyValue<String, TestObject>>) (key, value) -> {
                return new KeyValue<>(key, getTestObject(value));
            })
            .peek((key, value) -> System.out.println(key + " --- " + value))
            .selectKey((key, value) -> TimeUtils.extractWindowStart(value.getDate(), WINDOW_DURATION_SEC))
            .groupByKey(Grouped.with(Serdes.String(), new JsonPOJOSerde<>(TestObject.class)))
            .windowedBy(TimeWindows.of(Duration.of(5, ChronoUnit.MINUTES)))
            .aggregate(LinkedList::new,
                (Aggregator<String, TestObject, LinkedList<TestObject>>) (key, value, aggregate) -> addToLinkedList(value, aggregate))
            .toStream().map(new KeyValueMapper<Windowed<String>, LinkedList<TestObject>, KeyValue<Windowed<String>, String>>() {
            @Override
            public KeyValue<Windowed<String>, String> apply(Windowed<String> key, LinkedList<TestObject> value) {
                return new KeyValue<>(key, writeValueAsString(value));
            }
        }).to("output-stream0");

        KafkaStreams streams = new KafkaStreams(streamsBuilder.build(), config);

        final CountDownLatch latch = new CountDownLatch(1);

        Runtime.getRuntime().addShutdownHook(new Thread("streams-wordcount-shutdown-hook") {
            @Override
            public void run() {
                streams.close();
                latch.countDown();
            }
        });

        try {
            streams.start();
            latch.await();
        } catch (final Throwable e) {
            System.exit(1);
        }
        System.exit(0);

        /*new StreamsBuilder().stream("stream-test1", Consumed.with(Serdes.String(), Serdes.String()))
                .map((KeyValueMapper<String, String, KeyValue<String, TestObject>>) (key, value) -> new KeyValue<>(key, getTestObject(value)))
                .groupByKey()
                .windowedBy(TimeWindows.of(Duration.of(5, ChronoUnit.MINUTES)))
        ;*/

    }

    private static String writeValueAsString(Object o) {
        try {
            return mapper.writeValueAsString(o);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static LinkedList<TestObject> addToLinkedList(TestObject value, LinkedList<TestObject> aggregate) {
        aggregate.add(value);
        return aggregate;
    }

    private static long extractMyTimeFrom(ConsumerRecord<Object, Object> record) {
        return Stream.of(record.headers().toArray()).filter(h -> "mytime".equals(h.key()))
                .findAny()
                .map(Header::value).map(bs -> ByteBuffer.wrap(bs).getLong())
                .map(l -> TimeUtils.extractWindowStartDate(l, WINDOW_DURATION_SEC))
                .orElse(0L);
    }

    private static ObjectMapper mapper = new ObjectMapper();

    private static TestObject getTestObject(String value) {
        try {
            return mapper.readValue(value, TestObject.class);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
