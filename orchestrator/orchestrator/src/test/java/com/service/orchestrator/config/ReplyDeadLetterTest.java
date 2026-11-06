package com.service.orchestrator.config;

import com.service.orchestrator.AbstractPostgresTest;
import com.service.orchestrator.dto.ReplyOutcome;
import com.service.orchestrator.dto.SagaReply;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.test.context.EmbeddedKafka;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

import static com.service.orchestrator.config.KafkaTopics.*;
import static org.assertj.core.api.Assertions.assertThat;

@EmbeddedKafka(
        partitions = PARTITIONS,
        topics = {SAGA_REPLIES, SAGA_REPLIES_DLT},
        bootstrapServersProperty = "spring.kafka.bootstrap-servers")
class ReplyDeadLetterTest extends AbstractPostgresTest {

    private static final Duration WAIT = Duration.ofSeconds(30);

    @TestConfiguration
    static class ProbeConfig {

        @Bean
        ReplyProbe replyProbe() {
            return new ReplyProbe();
        }
    }

    static class ReplyProbe {

        final List<SagaReply> received = new CopyOnWriteArrayList<>();
        final Map<UUID, AtomicInteger> deliveries = new ConcurrentHashMap<>();

        @KafkaListener(id = "replyProbe", idIsGroup = false, topics = SAGA_REPLIES)
        void onReply(SagaReply reply) {
            deliveries.computeIfAbsent(reply.stepId(), id -> new AtomicInteger()).incrementAndGet();
            switch (String.valueOf(reply.reason())) {
                case "throw-permanent" -> throw new IllegalArgumentException("invalid edge");
                case "throw-transient" -> throw new IllegalStateException("database blip");
                default -> received.add(reply);
            }
        }
    }

    @Autowired
    private ReplyProbe probe;

    @Autowired
    @SuppressWarnings("SpringJavaInjectionPointsAutowiringInspection")
    private KafkaListenerEndpointRegistry listeners;

    @Value("${spring.kafka.bootstrap-servers}")
    private String brokers;

    private KafkaProducer<String, byte[]> producer;
    private KafkaConsumer<String, byte[]> dltConsumer;

    @BeforeEach
    void openClients() throws InterruptedException {
        awaitEveryConsumerAssigned();
        producer = new KafkaProducer<>(Map.of(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers),
                new StringSerializer(), new ByteArraySerializer());
        dltConsumer = new KafkaConsumer<>(Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers,
                ConsumerConfig.GROUP_ID_CONFIG, "dlt-reader-" + UUID.randomUUID(),
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"),
                new StringDeserializer(), new ByteArrayDeserializer());
        dltConsumer.subscribe(List.of(SAGA_REPLIES_DLT));
    }

    @AfterEach
    void closeClients() {
        producer.close();
        dltConsumer.close();
    }

    @Test
    void wellFormedReplyReachesTheListener() throws Exception {
        UUID stepId = UUID.randomUUID();
        String sagaId = UUID.randomUUID().toString();

        send(sagaId, replyJson(sagaId, stepId, "SUCCESS", null));

        SagaReply reply = awaitReceived(stepId);
        assertThat(reply.sagaId().toString()).isEqualTo(sagaId);
        assertThat(reply.outcome()).isEqualTo(ReplyOutcome.SUCCESS);
    }

    @Test
    void undeserializableReplyIsDeadLetteredWithItsOriginalBytes() {
        String sagaId = UUID.randomUUID().toString();
        byte[] garbage = "not json at all".getBytes(StandardCharsets.UTF_8);

        send(sagaId, garbage);

        ConsumerRecord<String, byte[]> dead = awaitDeadLetter(sagaId);
        assertThat(dead.value()).isEqualTo(garbage);
    }

    @Test
    void replyMissingRequiredFieldsIsDeadLettered() {
        String sagaId = UUID.randomUUID().toString();
        byte[] invalid = """
                {"sagaId":"%s","outcome":"SUCCESS"}
                """.formatted(sagaId).getBytes(StandardCharsets.UTF_8);

        send(sagaId, invalid);

        assertThat(awaitDeadLetter(sagaId).value()).isEqualTo(invalid);
    }

    @Test
    void permanentListenerFailureIsDeadLetteredWithoutRetry() {
        UUID stepId = UUID.randomUUID();
        String sagaId = UUID.randomUUID().toString();

        send(sagaId, replyJson(sagaId, stepId, "FAILED", "throw-permanent"));

        ConsumerRecord<String, byte[]> dead = awaitDeadLetter(sagaId);
        assertThat(new String(dead.value(), StandardCharsets.UTF_8)).contains(stepId.toString());
        assertThat(probe.deliveries.get(stepId).get()).isEqualTo(1);
    }

    @Test
    void transientListenerFailureIsRetriedThenDeadLettered() {
        UUID stepId = UUID.randomUUID();
        String sagaId = UUID.randomUUID().toString();

        send(sagaId, replyJson(sagaId, stepId, "FAILED", "throw-transient"));

        ConsumerRecord<String, byte[]> dead = awaitDeadLetter(sagaId);
        assertThat(new String(dead.value(), StandardCharsets.UTF_8)).contains(stepId.toString());
        assertThat(probe.deliveries.get(stepId).get()).isEqualTo(4);
    }

    private void awaitEveryConsumerAssigned() throws InterruptedException {
        ConcurrentMessageListenerContainer<?, ?> container =
                (ConcurrentMessageListenerContainer<?, ?>) listeners.getListenerContainer("replyProbe");
        long deadline = System.nanoTime() + WAIT.toNanos();
        while (System.nanoTime() < deadline) {
            if (container.getContainers().size() == PARTITIONS && container.getContainers().stream()
                    .allMatch(c -> c.getAssignedPartitions() != null && !c.getAssignedPartitions().isEmpty())) {
                return;
            }
            Thread.sleep(50);
        }
        throw new AssertionError("reply consumers were not all assigned a partition within " + WAIT);
    }

    private void send(String key, byte[] value) {
        try {
            producer.send(new ProducerRecord<>(SAGA_REPLIES, key, value)).get();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static byte[] replyJson(String sagaId, UUID stepId, String outcome, String reason) {
        String reasonJson = reason == null ? "null" : "\"" + reason + "\"";
        return """
                {"sagaId":"%s","stepId":"%s","commandId":"%s","action":"FORWARD","outcome":"%s","reason":%s}
                """.formatted(sagaId, stepId, UUID.randomUUID(), outcome, reasonJson)
                .getBytes(StandardCharsets.UTF_8);
    }

    private SagaReply awaitReceived(UUID stepId) throws InterruptedException {
        long deadline = System.nanoTime() + WAIT.toNanos();
        while (System.nanoTime() < deadline) {
            for (SagaReply reply : probe.received) {
                if (reply.stepId().equals(stepId)) {
                    return reply;
                }
            }
            Thread.sleep(50);
        }
        throw new AssertionError("reply for step " + stepId + " never reached the listener");
    }

    private ConsumerRecord<String, byte[]> awaitDeadLetter(String key) {
        long deadline = System.nanoTime() + WAIT.toNanos();
        while (System.nanoTime() < deadline) {
            for (ConsumerRecord<String, byte[]> record : dltConsumer.poll(Duration.ofMillis(200))) {
                if (key.equals(record.key())) {
                    return record;
                }
            }
        }
        throw new AssertionError("nothing with key " + key + " reached " + SAGA_REPLIES_DLT);
    }
}
