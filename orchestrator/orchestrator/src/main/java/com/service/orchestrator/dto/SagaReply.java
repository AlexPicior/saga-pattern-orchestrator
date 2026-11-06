package com.service.orchestrator.dto;

import org.springframework.util.Assert;

import java.util.UUID;

public record SagaReply(
        UUID sagaId,
        UUID stepId,
        UUID commandId,
        StepAction action,
        ReplyOutcome outcome,
        String reason) {

    public SagaReply {
        Assert.notNull(sagaId, "sagaId is required");
        Assert.notNull(stepId, "stepId is required");
        Assert.notNull(commandId, "commandId is required");
        Assert.notNull(action, "action is required");
        Assert.notNull(outcome, "outcome is required");
    }
}
