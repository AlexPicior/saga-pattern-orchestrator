package com.service.orchestrator.definition;

import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

@Component
public class SagaDefinitionRegistry {

    private final Map<String, SagaDefinition> byType;

    public SagaDefinitionRegistry(List<SagaDefinition> definitions) {
        this.byType = definitions.stream()
                .collect(Collectors.toUnmodifiableMap(SagaDefinition::type, Function.identity()));
    }

    public SagaDefinition get(String sagaType) {
        SagaDefinition definition = byType.get(sagaType);
        if (definition == null) {
            throw new IllegalArgumentException("No saga definition for type " + sagaType);
        }
        return definition;
    }
}
