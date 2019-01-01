package com.mikerusoft.testing;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.core.*;
import org.springframework.kafka.support.SendResult;
import org.springframework.kafka.support.serializer.JsonSerializer;
import org.springframework.util.concurrent.ListenableFutureCallback;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;

import java.time.Duration;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

@SpringBootApplication
@Configuration
@Slf4j
@EnableKafka
public class ProducerTest implements CommandLineRunner {

    public static void main(String[] args) {
        SpringApplication.run(ProducerTest.class, args);
    }

    @Bean
    public ProducerFactory<String, TestObject> producerFactory(
            @Value("${spring.kafka.bootstrap-servers}") String bootstrapAddress) {
        Map<String, Object> configProps = new HashMap<>();
        configProps.put(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
                bootstrapAddress);
        configProps.put(
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                StringSerializer.class);
        configProps.put(
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                JsonSerializer.class);
        configProps.put(
                ProducerConfig.ACKS_CONFIG,
                "all");
        return new DefaultKafkaProducerFactory<>(configProps);
    }

    @Bean
    public KafkaTemplate<String, TestObject> kafkaTemplate(ProducerFactory<String, TestObject> producerFactory) {
        return new KafkaTemplate<>(producerFactory);
    }

    @Autowired
    private KafkaTemplate<String, TestObject> kafkaTemplate;

    @Value("${spring.kafka.topic.name:stream-test}")
    private String topicName;

    @Value("${spring.kafka.produce.delayMs:500}")
    private long delayMs;

    @Override
    public void run(String... args) throws Exception {
        Disposable subscribe = Flux.interval(Duration.ofSeconds(1)).map(String::valueOf).delayElements(Duration.ofMillis(delayMs))
                .subscribe(this::sendData);
        while (!subscribe.isDisposed()) {
            Thread.sleep(100L);
        }
    }

    private static Random rand = new Random();

    private void sendData(String str) {
        TestObject recordValue = createObject(str);
        RecordHeaders headers = new RecordHeaders(new Header[]{new RecordHeader("mytime", ByteUtils.longToBytes(recordValue.getDate()))});
        ProducerRecord<String, TestObject> record = new ProducerRecord<>(topicName + rand.nextInt(2), null, (String)null, recordValue, headers);
        kafkaTemplate.send(record)
            .addCallback(new ListenableFutureCallback<SendResult<String, TestObject>>() {
                @Override
                public void onFailure(Throwable throwable) {
                    log.error("Error", throwable);
                }

                @Override
                public void onSuccess(SendResult<String, TestObject> result) {
                    log.info("Result for {} ", result.getProducerRecord());
                }
            });

    }

    private static TestObject createObject(String str) {
        return TestObject.builder().value("Test " + str).number(new Random().nextInt()).date(new Date(System.currentTimeMillis())).build();
    }
}
