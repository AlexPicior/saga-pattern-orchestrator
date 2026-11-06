package com.service.orchestrator.event;

import com.service.orchestrator.dto.SagaCommand;
import org.springframework.util.Assert;

public record CommandDispatchedEvent(String topic, SagaCommand command) {

    public CommandDispatchedEvent {
        Assert.hasText(topic, "topic is required");
        Assert.notNull(command, "command is required");
    }
}
