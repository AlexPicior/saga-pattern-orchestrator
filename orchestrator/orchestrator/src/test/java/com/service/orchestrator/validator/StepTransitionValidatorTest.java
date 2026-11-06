package com.service.orchestrator.validator;

import com.service.orchestrator.entity.StepStatus;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.Arrays;
import java.util.Set;
import java.util.stream.Stream;

import static com.service.orchestrator.entity.StepStatus.*;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class StepTransitionValidatorTest {

    private record Edge(StepStatus from, StepStatus to) {
    }

    // Written out separately from the production table on purpose: changing
    // the table without changing this list must fail a test.
    private static final Set<Edge> EXPECTED = Set.of(
            new Edge(PENDING, SENT),
            new Edge(SENT, SENT),
            new Edge(SENT, SUCCESS),
            new Edge(SENT, FAILED),
            new Edge(SENT, NEEDS_ATTENTION),
            new Edge(SUCCESS, COMPENSATING),
            new Edge(COMPENSATING, COMPENSATING),
            new Edge(COMPENSATING, COMPENSATED),
            new Edge(COMPENSATING, COMPENSATION_FAILED),
            new Edge(COMPENSATING, NEEDS_ATTENTION));

    static Stream<Arguments> allPairs() {
        return Arrays.stream(StepStatus.values())
                .flatMap(from -> Arrays.stream(StepStatus.values()).map(to -> Arguments.of(from, to)));
    }

    @ParameterizedTest(name = "{0} -> {1}")
    @MethodSource("allPairs")
    void everyPairMatchesTheWrittenOutTable(StepStatus from, StepStatus to) {
        assertThat(StepTransitionValidator.isValidTransition(from, to))
                .isEqualTo(EXPECTED.contains(new Edge(from, to)));
    }

    @Test
    void terminalStatusesAreExactlyTheOnesWithNoOutgoingEdge() {
        assertThat(Arrays.stream(StepStatus.values()).filter(StepTransitionValidator::isTerminal))
                .containsExactlyInAnyOrder(FAILED, COMPENSATED, COMPENSATION_FAILED, NEEDS_ATTENTION);
    }

    @Test
    void successIsNotTerminalBecauseItCanStillBeCompensated() {
        assertThat(StepTransitionValidator.isTerminal(SUCCESS)).isFalse();
    }

    @Test
    void requireValidThrowsOnInvalidEdge() {
        assertThatThrownBy(() -> StepTransitionValidator.requireValid(FAILED, SUCCESS))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("FAILED -> SUCCESS");
    }

    @Test
    void requireValidAcceptsValidEdge() {
        StepTransitionValidator.requireValid(PENDING, SENT);
    }
}