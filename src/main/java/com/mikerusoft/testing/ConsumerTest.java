package com.mikerusoft.testing;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.*;
import org.springframework.retry.RetryPolicy;
import org.springframework.retry.backoff.BackOffPolicy;
import org.springframework.retry.backoff.ExponentialBackOffPolicy;
import org.springframework.retry.policy.SimpleRetryPolicy;
import org.springframework.retry.support.RetryTemplate;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.apache.kafka.clients.consumer.ConsumerConfig.*;

@SpringBootApplication
@Configuration
@Slf4j
@EnableKafka
public class ConsumerTest {

    public static void main(String[] args) {
        SpringApplication.run(ConsumerTest.class, args);
    }

    @Bean
    public ConsumerFactory<String, byte[]> consumerFactory(
            @Value("${spring.kafka.bootstrap-servers}") String bootstrapAddress,
            @Value("${spring.kafka.consumer.group-id:STATUS-REPORT}") String groupId) {
        Map<String, Object> props = new HashMap<>();
        props.put(BOOTSTRAP_SERVERS_CONFIG, bootstrapAddress);
        props.put(GROUP_ID_CONFIG, groupId);
        props.put(AUTO_OFFSET_RESET_CONFIG, "earliest");
        return new DefaultKafkaConsumerFactory<>(props, new StringDeserializer(), new ByteArrayDeserializer());
        //return new DefaultKafkaConsumerFactory<>(props, new StringDeserializer(), new JsonDeserializer<>(TestObject.class));
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, byte[]> kafkaListenerContainerFactory(
            @Value("${spring.kafka.listener.concurrency:2}") Integer concurrency,
            ConsumerFactory<String, byte[]> consumerFactory, RetryTemplate retryTemplate) {
        ConcurrentKafkaListenerContainerFactory<String, byte[]>
                factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory);
        factory.setRetryTemplate(retryTemplate);
        factory.setConcurrency(concurrency);


        //factory.getContainerProperties().setAckMode(RECORD);
        return factory;
    }

    /**
     * Returns
     * @param retryInitialInterval the number of seconds to wait until the first retry
     * @param retryBackoffMultiplier the double value to multiply the interval after each retry
     * @param retryMaxInterval the maximal interval
     * @return returns the BackOffPolicy bean
     */
    @Bean
    public BackOffPolicy getBackOffPolicy(@Value("${common.kafka.listener.retry.initial-interval:1}") int retryInitialInterval,
                                          @Value("${common.kafka.listener.retry.backoff-multiplier:2}") double retryBackoffMultiplier,
                                          @Value("${common.kafka.listener.retry.max-interval:4}") int retryMaxInterval) {
        ExponentialBackOffPolicy backOffPolicy = new ExponentialBackOffPolicy();
        backOffPolicy.setInitialInterval(TimeUnit.SECONDS.toMillis(retryInitialInterval));
        backOffPolicy.setMultiplier(retryBackoffMultiplier);
        backOffPolicy.setMaxInterval(TimeUnit.SECONDS.toMillis(retryMaxInterval));
        // in case we want a fixed interval:
        // FixedBackOffPolicy backOffPolicy = new FixedBackOffPolicy();
        // backOffPolicy.setBackOffPeriod(1000);// constantly back off for 1 second between retries
        return backOffPolicy;
    }

    /**
     * Returns the retry policy - set to always retry (to avoid retry, a listener won't throw a RetryRequiredException)
     * @return returns the RetryPolicy bean
     */
    @Bean
    public RetryPolicy getKafkaRetryPolicy() {
        //return new AlwaysRetryPolicy();
        return new SimpleRetryPolicy(1);
    }

    /**
     * Returns the RetryTemplate bean for Kafka listeners. (combines the kafkaRetryPolicy with the kafkaBackoffPolicy)
     * @return the RetryTemplate bean for Kafka listeners.
     */
    @Bean
    public RetryTemplate getRetryOptions(BackOffPolicy backOffPolicy, RetryPolicy retryPolicy) {
        RetryTemplate retryTemplate = new RetryTemplate();
        retryTemplate.setRetryPolicy(retryPolicy);
        retryTemplate.setBackOffPolicy(backOffPolicy);
        return retryTemplate;
    }

    //@KafkaListener(topics = "${spring.kafka.topic.name:STATUS-REPORT}")
    public void receiveMessage(ConsumerRecord<String, byte[]> record) throws Exception {
        ObjectMapper mapper = new ObjectMapper();
        TestObject testObject = mapper.readValue(record.value(), TestObject.class);
        log.info(String.valueOf(testObject));
    }
}
