package com.service.orchestrator.definition;

import org.springframework.util.Assert;

import java.util.List;

public record SagaDefinition(String type, List<StepDefinition> steps) {

    public SagaDefinition {
        Assert.hasText(type, "type is required");
        Assert.notEmpty(steps, "a saga needs at least one step");
        steps = List.copyOf(steps);
        for (int i = 0; i < steps.size() - 1; i++) {
            Assert.isTrue(steps.get(i).isCompensatable(), "saga type " + type + ": only the last step may be the pivot, but "
                    + steps.get(i).name() + " is " + steps.get(i).kind());
        }
    }

    public StepDefinition step(int sequence) {
        Assert.isTrue(sequence >= 1 && sequence <= steps.size(),
                "sequence " + sequence + " is outside 1.." + steps.size() + " for saga type " + type);
        return steps.get(sequence - 1);
    }

    public int size() {
        return steps.size();
    }

    public boolean isLast(int sequence) {
        return sequence == steps.size();
    }
}
