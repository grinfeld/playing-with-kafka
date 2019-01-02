package com.mikerusoft.testing;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;

import java.nio.ByteBuffer;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.concurrent.CountDownLatch;

public class PlayWithKafkaStreams {

    private static final int WINDOW_DURATION_SEC = 1*60;

    public static void main(final String[] args) throws Exception {

        Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "stam-application13");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "kafka-1:9092");
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        config.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        StreamsBuilder streamsBuilder = new StreamsBuilder();
        defineStream(streamsBuilder);
        Topology topology = streamsBuilder.build();
        System.out.println("" + topology.describe());
        startTest(new KafkaStreams(topology, config));
    }

    private static void defineStream(StreamsBuilder streamsBuilder) {
        streamsBuilder.<String, String>stream("stream-test0", Consumed.with((record, previousTimestamp) -> extractMyTimeFrom(record)))
            .map((KeyValueMapper<String, String, KeyValue<String, TestObject>>) (key, value) -> new KeyValue<>(key, getTestObject(value)))
            .selectKey((key, value) -> TimeUtils.extractWindowStart(value.getDate(), WINDOW_DURATION_SEC))
            .groupByKey(Grouped.with(Serdes.String(), new JsonPOJOSerde<>(TestObject.class)))
            .windowedBy(TimeWindows.of(Duration.of(WINDOW_DURATION_SEC, ChronoUnit.SECONDS)))
            .aggregate(ArrayList::new,
                (key, value, aggregate) -> addToLinkedList(value, aggregate),
                Materialized.with(Serdes.String(), new JsonPOJOSerde<>(ArrayList.class))
            )
            .suppress(Suppressed.untilTimeLimit(Duration.of(WINDOW_DURATION_SEC, ChronoUnit.SECONDS), Suppressed.BufferConfig.unbounded()))
            .toStream()
            .map((KeyValueMapper<Windowed<String>, ArrayList, KeyValue<String, String>>)
                    PlayWithKafkaStreams::prepareDataToOutput)
                .peek((key, value) -> System.out.println(new Date(System.currentTimeMillis()) + value))
        .to("output-stream0");
    }

    private static void startTest(KafkaStreams streams) {
        final CountDownLatch latch = new CountDownLatch(1);

        Runtime.getRuntime().addShutdownHook(new Thread("streams-test-shutdown-hook") {
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
    }

    private static KeyValue<String, String> prepareDataToOutput(Windowed<String> key, ArrayList value) {
        Map<Object, Object> window = new HashMap<>();
        window.put(key.window().start() + " -- " + key.window().end(), value);
        return new KeyValue<>(key.key(), writeValueAsString(window));
    }

    private static String writeValueAsString(Object o) {
        try {
            return mapper.writeValueAsString(o);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static ArrayList addToLinkedList(Object value, ArrayList aggregate) {
        aggregate.add(value);
        return aggregate;
    }

    private static long extractMyTimeFrom(ConsumerRecord<Object, Object> record) {
        return ByteBuffer.wrap(record.headers().lastHeader("mytime").value()).getLong();
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
