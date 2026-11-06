package com.service.orchestrator.dto;

import org.springframework.util.Assert;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;

public record SagaCommand(
        UUID sagaId,
        UUID stepId,
        UUID commandId,
        StepAction action,
        int attempt,
        Map<String, Object> payload) {

    public SagaCommand {
        Assert.notNull(sagaId, "sagaId is required");
        Assert.notNull(stepId, "stepId is required");
        Assert.notNull(commandId, "commandId is required");
        Assert.notNull(action, "action is required");
        Assert.isTrue(attempt >= 1, "attempt must be >= 1");
        payload = payload == null ? Map.of() : Collections.unmodifiableMap(new LinkedHashMap<>(payload));
    }
}
