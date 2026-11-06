package com.service.orchestrator.messaging;

import com.service.orchestrator.config.KafkaTopics;
import com.service.orchestrator.dto.SagaCommand;
import com.service.orchestrator.dto.StepAction;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.core.KafkaTemplate;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class SagaCommandPublisherTest {

    @SuppressWarnings("unchecked")
    private final KafkaTemplate<String, SagaCommand> kafka = mock(KafkaTemplate.class);
    private final SagaCommandPublisher publisher = new SagaCommandPublisher(kafka);

    @Test
    void keysEveryCommandBySagaId() {
        when(kafka.send(anyString(), anyString(), any())).thenReturn(new CompletableFuture<>());
        SagaCommand command = new SagaCommand(UUID.randomUUID(), UUID.randomUUID(), UUID.randomUUID(),
                StepAction.FORWARD, 1, Map.of());

        publisher.publish(KafkaTopics.PAYMENT_COMMANDS, command);

        verify(kafka).send(KafkaTopics.PAYMENT_COMMANDS, command.sagaId().toString(), command);
    }

    @Test
    void failedSendSurfacesThroughTheReturnedFuture() {
        when(kafka.send(anyString(), anyString(), any()))
                .thenReturn(CompletableFuture.failedFuture(new RuntimeException("broker down")));
        SagaCommand command = new SagaCommand(UUID.randomUUID(), UUID.randomUUID(), UUID.randomUUID(),
                StepAction.FORWARD, 1, Map.of());

        assertThat(publisher.publish(KafkaTopics.PAYMENT_COMMANDS, command)).isCompletedExceptionally();
    }
}
