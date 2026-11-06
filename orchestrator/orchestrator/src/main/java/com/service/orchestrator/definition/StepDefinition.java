package com.service.orchestrator.definition;

import org.springframework.util.Assert;

import java.time.Duration;

public record StepDefinition(String name, String topic, Duration timeout, StepKind kind) {

    public StepDefinition {
        Assert.hasText(name, "name is required");
        Assert.hasText(topic, "topic is required");
        Assert.notNull(timeout, "timeout is required");
        Assert.isTrue(!timeout.isNegative() && !timeout.isZero(), "timeout must be positive");
        Assert.notNull(kind, "kind is required");
    }

    public boolean isCompensatable() {
        return kind == StepKind.COMPENSATABLE;
    }
}
