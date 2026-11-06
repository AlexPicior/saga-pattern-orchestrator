package com.service.orchestrator.definition;

import com.service.orchestrator.config.SagaDefinitionConfig;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import static com.service.orchestrator.config.KafkaTopics.*;
import static com.service.orchestrator.definition.StepKind.COMPENSATABLE;
import static com.service.orchestrator.definition.StepKind.PIVOT;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class SagaDefinitionTest {

    private static final Duration TIMEOUT = Duration.ofSeconds(30);

    private final SagaDefinition order = new SagaDefinitionConfig().orderSagaDefinition(TIMEOUT);

    @Test
    void orderSagaRunsInventoryThenPaymentThenShipping() {
        assertThat(order.type()).isEqualTo(SagaDefinitionConfig.ORDER_SAGA);
        assertThat(order.steps()).extracting(StepDefinition::topic)
                .containsExactly(INVENTORY_COMMANDS, PAYMENT_COMMANDS, SHIPPING_COMMANDS);
        assertThat(order.steps()).extracting(StepDefinition::timeout).containsOnly(TIMEOUT);
    }

    @Test
    void shippingIsThePivotOfTheOrderSaga() {
        assertThat(order.steps()).extracting(StepDefinition::kind)
                .containsExactly(COMPENSATABLE, COMPENSATABLE, PIVOT);
        assertThat(order.step(3).isCompensatable()).isFalse();
    }

    @Test
    void stepsAreOneBased() {
        assertThat(order.step(1).name()).isEqualTo("RESERVE_INVENTORY");
        assertThat(order.step(3).name()).isEqualTo("SHIP_ORDER");
        assertThat(order.isLast(3)).isTrue();
        assertThat(order.isLast(2)).isFalse();
    }

    @Test
    void sequenceOutsideTheDefinitionIsRejected() {
        assertThatThrownBy(() -> order.step(0)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> order.step(4)).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void definitionNeedsATypeAndAtLeastOneStep() {
        assertThatThrownBy(() -> new SagaDefinition("ORDER", List.of()))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new SagaDefinition(" ", List.of(step("A", COMPENSATABLE))))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void stepsCannotBeChangedAfterConstruction() {
        List<StepDefinition> steps = new ArrayList<>(List.of(step("A", COMPENSATABLE)));
        SagaDefinition definition = new SagaDefinition("T", steps);
        steps.add(step("B", COMPENSATABLE));

        assertThat(definition.size()).isEqualTo(1);
    }

    @Test
    void stepTimeoutMustBePositive() {
        assertThatThrownBy(() -> new StepDefinition("A", "a.commands", Duration.ZERO, COMPENSATABLE))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void stepKindIsRequired() {
        assertThatThrownBy(() -> new StepDefinition("A", "a.commands", TIMEOUT, null))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void pivotMayOnlyBeTheLastStep() {
        assertThatThrownBy(() -> new SagaDefinition("T", List.of(step("A", PIVOT), step("B", COMPENSATABLE))))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("only the last step may be the pivot");
    }

    @Test
    void sagaWithoutAPivotIsAllowed() {
        SagaDefinition definition = new SagaDefinition("T", List.of(step("A", COMPENSATABLE), step("B", COMPENSATABLE)));

        assertThat(definition.steps()).allMatch(StepDefinition::isCompensatable);
    }

    private static StepDefinition step(String name, StepKind kind) {
        return new StepDefinition(name, name.toLowerCase() + ".commands", TIMEOUT, kind);
    }
}
