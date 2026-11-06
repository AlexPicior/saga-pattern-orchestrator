package com.service.orchestrator.service;

import com.service.orchestrator.AbstractPostgresTest;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

import java.time.Duration;
import java.util.UUID;

import static com.service.orchestrator.entity.StepStatus.*;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class StepTransitionServiceTest extends AbstractPostgresTest {

    private static final Duration TIMEOUT = Duration.ofSeconds(30);

    @Autowired
    private StepTransitionService service;

    @Test
    void dispatchPendingToSentStartsAttemptsAndArmsTimeoutFromDbClock() {
        UUID stepId = seedStep(PENDING, 0);
        StepRow before = readStep(stepId);

        assertThat(service.dispatch(stepId, PENDING, SENT, TIMEOUT)).isTrue();

        StepRow after = readStep(stepId);
        assertThat(after.status()).isEqualTo(SENT);
        assertThat(after.attemptCount()).isEqualTo(1);
        assertThat(after.sentAt()).isNotNull();
        assertThat(Duration.between(after.sentAt(), after.timeoutAt())).isEqualTo(TIMEOUT);
        assertThat(after.updatedAt()).isAfter(before.updatedAt());
        assertThat(after.commandId()).isEqualTo(before.commandId());
    }

    @Test
    void dispatchSuccessToCompensatingRestartsAttemptCountAtOne() {
        UUID stepId = seedStep(SUCCESS, 3);

        assertThat(service.dispatch(stepId, SUCCESS, COMPENSATING, TIMEOUT)).isTrue();

        StepRow after = readStep(stepId);
        assertThat(after.status()).isEqualTo(COMPENSATING);
        assertThat(after.attemptCount()).isEqualTo(1);
        assertThat(Duration.between(after.sentAt(), after.timeoutAt())).isEqualTo(TIMEOUT);
    }

    @Test
    void enteringCompensatingGeneratesADistinctCompensationCommandId() {
        UUID stepId = seedStep(SUCCESS, 1);
        UUID forwardCommandId = readStep(stepId).commandId();
        assertThat(readStep(stepId).compensationCommandId()).isNull();

        service.dispatch(stepId, SUCCESS, COMPENSATING, TIMEOUT);

        StepRow after = readStep(stepId);
        assertThat(after.compensationCommandId()).isNotNull().isNotEqualTo(forwardCommandId);
        assertThat(after.commandId()).isEqualTo(forwardCommandId);
    }

    @Test
    void forwardDispatchLeavesCompensationCommandIdEmpty() {
        UUID stepId = seedStep(PENDING, 0);

        service.dispatch(stepId, PENDING, SENT, TIMEOUT);

        assertThat(readStep(stepId).compensationCommandId()).isNull();
    }

    @Test
    void compensationResendKeepsTheSameCompensationCommandId() {
        UUID stepId = seedStep(SUCCESS, 1);
        service.dispatch(stepId, SUCCESS, COMPENSATING, TIMEOUT);
        UUID compensationCommandId = readStep(stepId).compensationCommandId();

        assertThat(service.redispatch(stepId, COMPENSATING, 1, TIMEOUT)).isTrue();

        assertThat(readStep(stepId).compensationCommandId()).isEqualTo(compensationCommandId);
    }

    @Test
    void dispatchSupportsSubSecondTimeouts() {
        UUID stepId = seedStep(PENDING, 0);

        service.dispatch(stepId, PENDING, SENT, Duration.ofMillis(1500));

        StepRow after = readStep(stepId);
        assertThat(Duration.between(after.sentAt(), after.timeoutAt())).isEqualTo(Duration.ofMillis(1500));
    }

    @Test
    void dispatchLosesWhenRowIsNoLongerInFromStatus() {
        UUID stepId = seedStep(SENT, 1);
        StepRow before = readStep(stepId);

        assertThat(service.dispatch(stepId, PENDING, SENT, TIMEOUT)).isFalse();
        assertThat(readStep(stepId)).isEqualTo(before);
    }

    @Test
    void dispatchRefusesEdgesThatAreNotFirstSends() {
        UUID stepId = seedStep(SENT, 1);

        assertThatThrownBy(() -> service.dispatch(stepId, SENT, SUCCESS, TIMEOUT))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> service.dispatch(stepId, SENT, SENT, TIMEOUT))
                .isInstanceOf(IllegalArgumentException.class);
        assertThat(readStep(stepId).status()).isEqualTo(SENT);
    }

    @Test
    void dispatchRejectsNonPositiveTimeout() {
        UUID stepId = seedStep(PENDING, 0);

        assertThatThrownBy(() -> service.dispatch(stepId, PENDING, SENT, Duration.ZERO))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> service.dispatch(stepId, PENDING, SENT, Duration.ofSeconds(-1)))
                .isInstanceOf(IllegalArgumentException.class);
        assertThat(readStep(stepId).status()).isEqualTo(PENDING);
    }

    @Test
    void transitionChangesOnlyStatusAndUpdatedAt() {
        UUID stepId = seedStep(PENDING, 0);
        service.dispatch(stepId, PENDING, SENT, TIMEOUT);
        StepRow before = readStep(stepId);

        assertThat(service.transition(stepId, SENT, SUCCESS)).isTrue();

        StepRow after = readStep(stepId);
        assertThat(after.status()).isEqualTo(SUCCESS);
        assertThat(after.attemptCount()).isEqualTo(before.attemptCount());
        assertThat(after.sentAt()).isEqualTo(before.sentAt());
        assertThat(after.timeoutAt()).isEqualTo(before.timeoutAt());
        assertThat(after.commandId()).isEqualTo(before.commandId());
        assertThat(after.updatedAt()).isAfterOrEqualTo(before.updatedAt());
    }

    @Test
    void transitionCoversEveryPlainEdge() {
        UUID failed = seedStep(SENT, 1);
        UUID compensated = seedStep(COMPENSATING, 1);
        UUID compensationFailed = seedStep(COMPENSATING, 2);

        assertThat(service.transition(failed, SENT, FAILED)).isTrue();
        assertThat(service.transition(compensated, COMPENSATING, COMPENSATED)).isTrue();
        assertThat(service.transition(compensationFailed, COMPENSATING, COMPENSATION_FAILED)).isTrue();

        assertThat(readStep(failed).status()).isEqualTo(FAILED);
        assertThat(readStep(compensated).status()).isEqualTo(COMPENSATED);
        assertThat(readStep(compensationFailed).status()).isEqualTo(COMPENSATION_FAILED);
    }

    @Test
    void transitionLosesWhenRowIsNoLongerInFromStatus() {
        UUID stepId = seedStep(SUCCESS, 1);
        StepRow before = readStep(stepId);

        assertThat(service.transition(stepId, SENT, FAILED)).isFalse();
        assertThat(readStep(stepId)).isEqualTo(before);
    }

    @Test
    void transitionOnUnknownStepReturnsFalse() {
        assertThat(service.transition(UUID.randomUUID(), SENT, SUCCESS)).isFalse();
    }

    @Test
    void transitionRefusesSendEdges() {
        UUID pending = seedStep(PENDING, 0);
        UUID success = seedStep(SUCCESS, 1);
        UUID sent = seedStep(SENT, 1);

        assertThatThrownBy(() -> service.transition(pending, PENDING, SENT))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("dispatch");
        assertThatThrownBy(() -> service.transition(success, SUCCESS, COMPENSATING))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> service.transition(sent, SENT, SENT))
                .isInstanceOf(IllegalArgumentException.class);

        assertThat(readStep(pending).status()).isEqualTo(PENDING);
        assertThat(readStep(success).status()).isEqualTo(SUCCESS);
        assertThat(readStep(sent).status()).isEqualTo(SENT);
    }

    @Test
    void invalidEdgeThrowsAndLeavesRowUntouched() {
        UUID stepId = seedStep(FAILED, 1);
        StepRow before = readStep(stepId);

        assertThatThrownBy(() -> service.transition(stepId, FAILED, SUCCESS))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("FAILED -> SUCCESS");
        assertThat(readStep(stepId)).isEqualTo(before);
    }

    @Test
    void redispatchIncrementsAttemptAndReArmsTimeoutKeepingCommandId() {
        UUID stepId = seedStep(PENDING, 0);
        service.dispatch(stepId, PENDING, SENT, TIMEOUT);
        StepRow before = readStep(stepId);

        assertThat(service.redispatch(stepId, SENT, 1, TIMEOUT)).isTrue();

        StepRow after = readStep(stepId);
        assertThat(after.status()).isEqualTo(SENT);
        assertThat(after.attemptCount()).isEqualTo(2);
        assertThat(after.commandId()).isEqualTo(before.commandId());
        assertThat(after.sentAt()).isAfter(before.sentAt());
        assertThat(after.timeoutAt()).isAfter(before.timeoutAt());
        assertThat(Duration.between(after.sentAt(), after.timeoutAt())).isEqualTo(TIMEOUT);
    }

    @Test
    void redispatchWorksForCompensating() {
        UUID stepId = seedStep(COMPENSATING, 1);

        assertThat(service.redispatch(stepId, COMPENSATING, 1, TIMEOUT)).isTrue();

        StepRow after = readStep(stepId);
        assertThat(after.status()).isEqualTo(COMPENSATING);
        assertThat(after.attemptCount()).isEqualTo(2);
    }

    @Test
    void redispatchLosesOnStaleAttemptCount() {
        UUID stepId = seedStep(SENT, 2);
        StepRow before = readStep(stepId);

        assertThat(service.redispatch(stepId, SENT, 1, TIMEOUT)).isFalse();
        assertThat(readStep(stepId)).isEqualTo(before);
    }

    @Test
    void escalateMovesSentAndCompensatingToNeedsAttention() {
        UUID sent = seedStep(SENT, 5);
        UUID compensating = seedStep(COMPENSATING, 5);
        StepRow before = readStep(sent);

        assertThat(service.escalate(sent, SENT, 5)).isTrue();
        assertThat(service.escalate(compensating, COMPENSATING, 5)).isTrue();

        StepRow after = readStep(sent);
        assertThat(after.status()).isEqualTo(NEEDS_ATTENTION);
        assertThat(after.attemptCount()).isEqualTo(5);
        assertThat(after.commandId()).isEqualTo(before.commandId());
        assertThat(readStep(compensating).status()).isEqualTo(NEEDS_ATTENTION);
    }

    @Test
    void escalateLosesOnStaleAttemptCountOrStatus() {
        UUID resent = seedStep(SENT, 3);
        UUID answered = seedStep(SUCCESS, 2);

        assertThat(service.escalate(resent, SENT, 2)).isFalse();
        assertThat(service.escalate(answered, SENT, 2)).isFalse();

        assertThat(readStep(resent).status()).isEqualTo(SENT);
        assertThat(readStep(answered).status()).isEqualTo(SUCCESS);
    }

    @Test
    void escalateRefusesStatusesThatAreNotInFlight() {
        UUID pending = seedStep(PENDING, 0);

        assertThatThrownBy(() -> service.escalate(pending, PENDING, 0))
                .isInstanceOf(IllegalArgumentException.class);
        assertThat(readStep(pending).status()).isEqualTo(PENDING);
    }

    @Test
    void transitionRefusesEscalation() {
        UUID sent = seedStep(SENT, 1);

        assertThatThrownBy(() -> service.transition(sent, SENT, NEEDS_ATTENTION))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("escalate");
        assertThat(readStep(sent).status()).isEqualTo(SENT);
    }

    @Test
    void redispatchRefusesStatusesWithoutAResendEdge() {
        UUID pending = seedStep(PENDING, 0);
        UUID success = seedStep(SUCCESS, 1);

        assertThatThrownBy(() -> service.redispatch(pending, PENDING, 0, TIMEOUT))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> service.redispatch(success, SUCCESS, 1, TIMEOUT))
                .isInstanceOf(IllegalArgumentException.class);

        assertThat(readStep(pending).attemptCount()).isZero();
        assertThat(readStep(success).attemptCount()).isEqualTo(1);
    }
}
