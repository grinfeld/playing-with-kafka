package com.mikerusoft.testing;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Properties;
import java.util.stream.Stream;

public class KafkaStreams {
    public static void main(final String[] args) throws Exception {

        Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "stam-application");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "kafka-broker1:9092");
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        new StreamsBuilder()
            .stream("stream-test0", Consumed.<String, String>with((record, previousTimestamp) -> extractMyTimeFrom(record)))
            .map((KeyValueMapper<String, String, KeyValue<String, TestObject>>) (key, value) -> new KeyValue<>(key, getTestObject(value)))
            .peek((key, value) -> System.out.println(key + " --- " + value))

        ;


        /*new StreamsBuilder().stream("stream-test1", Consumed.with(Serdes.String(), Serdes.String()))
                .map((KeyValueMapper<String, String, KeyValue<String, TestObject>>) (key, value) -> new KeyValue<>(key, getTestObject(value)))
                .groupByKey()
                .windowedBy(TimeWindows.of(Duration.of(5, ChronoUnit.MINUTES)))
        ;*/

    }

    private static long extractMyTimeFrom(ConsumerRecord<Object, Object> record) {
        return Stream.of(record.headers().toArray()).filter(h -> "mytime".equals(h.key()))
                .findAny()
                .map(Header::value).map(ByteUtils::bytesToLong)
                .map(l -> TimeUtils.extractWindowStartDate(l, 60))
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
