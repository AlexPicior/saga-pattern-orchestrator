package com.service.orchestrator.config;

import com.service.orchestrator.failure.FailureClassifier;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.support.serializer.DelegatingByTypeSerializer;
import org.springframework.kafka.support.serializer.JacksonJsonSerializer;
import org.springframework.util.backoff.FixedBackOff;

import java.util.LinkedHashMap;
import java.util.Map;

import static com.service.orchestrator.config.KafkaTopics.*;

@Configuration(proxyBeanMethods = false)
public class KafkaConfig {

    private static final long RETRY_INTERVAL_MS = 1000L;
    private static final long MAX_RETRIES = 3L;

    @Bean
    KafkaAdmin.NewTopics sagaTopics() {
        return new KafkaAdmin.NewTopics(
                topic(INVENTORY_COMMANDS),
                topic(PAYMENT_COMMANDS),
                topic(SHIPPING_COMMANDS),
                topic(SAGA_REPLIES),
                topic(SAGA_REPLIES_DLT));
    }

    @Bean
    DefaultErrorHandler kafkaErrorHandler(ProducerFactory<?, ?> producerFactory, FailureClassifier classifier) {
        DefaultErrorHandler handler = new DefaultErrorHandler(
                new DeadLetterPublishingRecoverer(deadLetterTemplate(producerFactory)),
                new FixedBackOff(RETRY_INTERVAL_MS, MAX_RETRIES));
        Map<Class<? extends Throwable>, Boolean> retryable = new LinkedHashMap<>();
        classifier.permanentExceptions().forEach(type -> retryable.put(type, false));
        handler.setClassifications(retryable, true);
        return handler;
    }

    private static KafkaTemplate<String, Object> deadLetterTemplate(ProducerFactory<?, ?> producerFactory) {
        Map<Class<?>, Serializer<?>> byType = new LinkedHashMap<>();
        byType.put(byte[].class, new ByteArraySerializer());
        byType.put(Object.class, new JacksonJsonSerializer<>());
        return new KafkaTemplate<>(new DefaultKafkaProducerFactory<>(
                producerFactory.getConfigurationProperties(),
                new StringSerializer(),
                new DelegatingByTypeSerializer(byType, true)));
    }

    private static NewTopic topic(String name) {
        return TopicBuilder.name(name).partitions(PARTITIONS).replicas(1).build();
    }
}
