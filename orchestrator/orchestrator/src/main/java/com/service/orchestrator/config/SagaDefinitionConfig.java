package com.service.orchestrator.config;

import com.service.orchestrator.definition.SagaDefinition;
import com.service.orchestrator.definition.StepDefinition;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.time.Duration;
import java.util.List;

import static com.service.orchestrator.config.KafkaTopics.*;
import static com.service.orchestrator.definition.StepKind.COMPENSATABLE;
import static com.service.orchestrator.definition.StepKind.PIVOT;

@Configuration(proxyBeanMethods = false)
public class SagaDefinitionConfig {

    public static final String ORDER_SAGA = "ORDER";

    @Bean
    public SagaDefinition orderSagaDefinition(@Value("${saga.step-timeout:30s}") Duration stepTimeout) {
        return new SagaDefinition(ORDER_SAGA, List.of(
                new StepDefinition("RESERVE_INVENTORY", INVENTORY_COMMANDS, stepTimeout, COMPENSATABLE),
                new StepDefinition("PROCESS_PAYMENT", PAYMENT_COMMANDS, stepTimeout, COMPENSATABLE),
                new StepDefinition("SHIP_ORDER", SHIPPING_COMMANDS, stepTimeout, PIVOT)));
    }
}
