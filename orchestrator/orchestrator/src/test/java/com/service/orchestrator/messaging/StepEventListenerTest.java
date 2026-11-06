package com.service.orchestrator.messaging;

import com.service.orchestrator.AbstractPostgresTest;
import com.service.orchestrator.entity.SagaStatus;
import com.service.orchestrator.entity.StepStatus;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.kafka.test.context.EmbeddedKafka;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.BooleanSupplier;

import static com.service.orchestrator.config.KafkaTopics.*;
import static com.service.orchestrator.config.SagaDefinitionConfig.ORDER_SAGA;
import static org.assertj.core.api.Assertions.assertThat;

@EmbeddedKafka(
        partitions = PARTITIONS,
        topics = {SAGA_REPLIES, SAGA_REPLIES_DLT, INVENTORY_COMMANDS, PAYMENT_COMMANDS, SHIPPING_COMMANDS},
        bootstrapServersProperty = "spring.kafka.bootstrap-servers")
class StepEventListenerTest extends AbstractPostgresTest {

    private static final Duration WAIT = Duration.ofSeconds(30);

    @Autowired
    @SuppressWarnings("SpringJavaInjectionPointsAutowiringInspection")
    private KafkaListenerEndpointRegistry registry;

    @Value("${spring.kafka.bootstrap-servers}")
    private String brokers;

    private final JsonMapper mapper = JsonMapper.builder().build();

    private KafkaProducer<String, String> producer;
    private KafkaConsumer<String, String> paymentCommands;
    private KafkaConsumer<String, String> deadLetters;

    @BeforeEach
    void startListenerAndClients() {
        MessageListenerContainer container = registry.getListenerContainer(StepEventListener.ID);
        if (!container.isRunning()) {
            container.start();
        }
        producer = new KafkaProducer<>(Map.of(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers),
                new StringSerializer(), new StringSerializer());
        paymentCommands = consumer(PAYMENT_COMMANDS);
        deadLetters = consumer(SAGA_REPLIES_DLT);
    }

    @AfterEach
    void closeClients() {
        producer.close();
        paymentCommands.close();
        deadLetters.close();
    }

    @Test
    void successReplyAdvancesTheSagaAndPublishesTheNextCommand() throws Exception {
        UUID sagaId = seedSaga(ORDER_SAGA, SagaStatus.STARTED, 1, """
                {"orderId":"o-42"}
                """);
        UUID stepId = seedStepAt(sagaId, 1, StepStatus.SENT);
        UUID commandId = readStep(stepId).commandId();

        sendReply(sagaId, """
                {"sagaId":"%s","stepId":"%s","commandId":"%s","action":"FORWARD","outcome":"SUCCESS","reason":null}
                """.formatted(sagaId, stepId, commandId));

        await(() -> readSaga(sagaId).currentStepSequence() == 2, "saga never advanced to step 2");
        assertThat(readStep(stepId).status()).isEqualTo(StepStatus.SUCCESS);
        UUID nextStepId = stepIdAt(sagaId, 2);

        ConsumerRecord<String, String> record = awaitRecord(paymentCommands, sagaId.toString());
        JsonNode command = mapper.readTree(record.value());
        assertThat(command.get("stepId").asString()).isEqualTo(nextStepId.toString());
        assertThat(command.get("commandId").asString()).isEqualTo(readStep(nextStepId).commandId().toString());
        assertThat(command.get("action").asString()).isEqualTo("FORWARD");
        assertThat(command.get("attempt").asInt()).isEqualTo(1);
        assertThat(command.get("payload").get("orderId").asString()).isEqualTo("o-42");
        assertThat(record.headers().lastHeader("__TypeId__")).isNull();
    }

    @Test
    void replyForAnUnknownStepIsDeadLettered() {
        UUID sagaId = UUID.randomUUID();
        String reply = """
                {"sagaId":"%s","stepId":"%s","commandId":"%s","action":"FORWARD","outcome":"SUCCESS","reason":null}
                """.formatted(sagaId, UUID.randomUUID(), UUID.randomUUID());

        sendReply(sagaId, reply);

        assertThat(mapper.readTree(awaitRecord(deadLetters, sagaId.toString()).value()))
                .isEqualTo(mapper.readTree(reply));
    }

    private KafkaConsumer<String, String> consumer(String topic) {
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers,
                ConsumerConfig.GROUP_ID_CONFIG, "test-" + UUID.randomUUID(),
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"),
                new StringDeserializer(), new StringDeserializer());
        consumer.subscribe(List.of(topic));
        return consumer;
    }

    private void sendReply(UUID sagaId, String json) {
        try {
            producer.send(new ProducerRecord<>(SAGA_REPLIES, sagaId.toString(), json)).get();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static ConsumerRecord<String, String> awaitRecord(KafkaConsumer<String, String> consumer, String key) {
        long deadline = System.nanoTime() + WAIT.toNanos();
        while (System.nanoTime() < deadline) {
            for (ConsumerRecord<String, String> record : consumer.poll(Duration.ofMillis(200))) {
                if (key.equals(record.key())) {
                    return record;
                }
            }
        }
        throw new AssertionError("no record with key " + key);
    }

    private static void await(BooleanSupplier condition, String failure) throws InterruptedException {
        long deadline = System.nanoTime() + WAIT.toNanos();
        while (System.nanoTime() < deadline) {
            if (condition.getAsBoolean()) {
                return;
            }
            Thread.sleep(100);
        }
        throw new AssertionError(failure);
    }
}
