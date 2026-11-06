package com.service.orchestrator.validator;

import com.service.orchestrator.entity.SagaStatus;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.Arrays;
import java.util.Set;
import java.util.stream.Stream;

import static com.service.orchestrator.entity.SagaStatus.*;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class SagaTransitionValidatorTest {

    private record Edge(SagaStatus from, SagaStatus to) {
    }

    private static final Set<Edge> EXPECTED = Set.of(
            new Edge(STARTED, COMPLETED),
            new Edge(STARTED, COMPENSATING),
            new Edge(STARTED, NEEDS_ATTENTION),
            new Edge(COMPENSATING, FAILED),
            new Edge(COMPENSATING, COMPENSATION_FAILED),
            new Edge(COMPENSATING, NEEDS_ATTENTION));

    static Stream<Arguments> allPairs() {
        return Arrays.stream(SagaStatus.values())
                .flatMap(from -> Arrays.stream(SagaStatus.values()).map(to -> Arguments.of(from, to)));
    }

    @ParameterizedTest(name = "{0} -> {1}")
    @MethodSource("allPairs")
    void everyPairMatchesTheWrittenOutTable(SagaStatus from, SagaStatus to) {
        assertThat(SagaTransitionValidator.isValidTransition(from, to))
                .isEqualTo(EXPECTED.contains(new Edge(from, to)));
    }

    @Test
    void terminalStatusesAreExactlyTheOnesWithNoOutgoingEdge() {
        assertThat(Arrays.stream(SagaStatus.values()).filter(SagaTransitionValidator::isTerminal))
                .containsExactlyInAnyOrder(COMPLETED, FAILED, COMPENSATION_FAILED, NEEDS_ATTENTION);
    }

    @Test
    void failureCannotSkipCompensating() {
        assertThat(SagaTransitionValidator.isValidTransition(STARTED, FAILED)).isFalse();
    }

    @Test
    void requireValidThrowsOnInvalidEdge() {
        assertThatThrownBy(() -> SagaTransitionValidator.requireValid(COMPLETED, COMPENSATING))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("COMPLETED -> COMPENSATING");
    }

    @Test
    void requireValidAcceptsValidEdge() {
        SagaTransitionValidator.requireValid(STARTED, COMPLETED);
    }
}
