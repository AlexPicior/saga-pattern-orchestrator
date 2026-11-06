package com.service.orchestrator.messaging;

import com.service.orchestrator.dto.SagaCommand;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;

import java.util.concurrent.CompletableFuture;

@Component
public class SagaCommandPublisher {

    private static final Logger log = LoggerFactory.getLogger(SagaCommandPublisher.class);

    private final KafkaTemplate<String, SagaCommand> kafka;

    public SagaCommandPublisher(KafkaTemplate<String, SagaCommand> kafka) {
        this.kafka = kafka;
    }

    public CompletableFuture<SendResult<String, SagaCommand>> publish(String topic, SagaCommand command) {
        return kafka.send(topic, command.sagaId().toString(), command)
                .whenComplete((result, ex) -> {
                    if (ex != null) {
                        log.warn("Publish of command {} ({} attempt {}) for saga {} to {} failed; the timeout poller will resend",
                                command.commandId(), command.action(), command.attempt(), command.sagaId(), topic, ex);
                    }
                });
    }
}
