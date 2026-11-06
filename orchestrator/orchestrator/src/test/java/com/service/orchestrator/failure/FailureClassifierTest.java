package com.service.orchestrator.failure;

import com.service.orchestrator.dto.ReplyOutcome;
import com.service.orchestrator.dto.SagaReply;
import com.service.orchestrator.dto.StepAction;
import org.apache.kafka.common.KafkaException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.springframework.dao.CannotAcquireLockException;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.dao.QueryTimeoutException;
import org.springframework.kafka.listener.ListenerExecutionFailedException;
import org.springframework.kafka.support.serializer.DeserializationException;
import org.springframework.messaging.converter.MessageConversionException;

import java.util.UUID;
import java.util.concurrent.TimeoutException;
import java.util.stream.Stream;

import static com.service.orchestrator.failure.FailureKind.*;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class FailureClassifierTest {

    private final FailureClassifier classifier = new FailureClassifier();

    @ParameterizedTest
    @EnumSource(StepAction.class)
    void failedReplyIsABusinessFailure(StepAction action) {
        StepFailure failure = new StepFailure.ReplyFailed(reply(action, ReplyOutcome.FAILED));

        assertThat(classifier.classify(failure)).isEqualTo(BUSINESS);
    }

    @Test
    void successReplyCannotBeWrappedAsAFailure() {
        assertThatThrownBy(() -> new StepFailure.ReplyFailed(reply(StepAction.FORWARD, ReplyOutcome.SUCCESS)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("SUCCESS");
    }

    static Stream<Arguments> timeouts() {
        return Stream.of(StepAction.values())
                .flatMap(action -> Stream.of(1, 2, 5, 100, Integer.MAX_VALUE).map(attempt -> Arguments.of(action, attempt)));
    }

    @ParameterizedTest(name = "{0} attempt {1}")
    @MethodSource("timeouts")
    void timeoutIsNeverABusinessFailure(StepAction action, int attempt) {
        FailureKind kind = classifier.classify(new StepFailure.TimedOut(action, attempt));

        assertThat(kind).isEqualTo(TRANSIENT);
        assertThat(kind).isNotEqualTo(BUSINESS);
    }

    static Stream<Throwable> permanentErrors() {
        return Stream.of(
                new DeserializationException("bad bytes", new byte[]{1, 2}, false, new RuntimeException()),
                new MessageConversionException("cannot convert"),
                new IllegalArgumentException("Invalid step transition: FAILED -> SUCCESS"),
                new ClassCastException(),
                new ListenerExecutionFailedException("listener failed", new IllegalArgumentException("wrapped")),
                new RuntimeException(new RuntimeException(
                        new DeserializationException("deep", new byte[0], false, null))));
    }

    @ParameterizedTest
    @MethodSource("permanentErrors")
    void malformedInputIsPermanent(Throwable error) {
        assertThat(classifier.classify(new StepFailure.Thrown(error))).isEqualTo(PERMANENT);
    }

    static Stream<Throwable> transientErrors() {
        return Stream.of(
                new CannotAcquireLockException("lock"),
                new QueryTimeoutException("slow"),
                new DataIntegrityViolationException("duplicate key"),
                new KafkaException("broker unavailable"),
                new TimeoutException("send timed out"),
                new IllegalStateException("unexpected"),
                new ListenerExecutionFailedException("listener failed", new CannotAcquireLockException("lock")));
    }

    @ParameterizedTest
    @MethodSource("transientErrors")
    void infrastructureAndUnknownErrorsAreTransient(Throwable error) {
        assertThat(classifier.classify(new StepFailure.Thrown(error))).isEqualTo(TRANSIENT);
    }

    @ParameterizedTest
    @MethodSource({"permanentErrors", "transientErrors"})
    void exceptionIsNeverABusinessFailure(Throwable error) {
        assertThat(classifier.classify(new StepFailure.Thrown(error))).isNotEqualTo(BUSINESS);
    }

    @Test
    void causeCycleTerminates() {
        RuntimeException a = new RuntimeException("a");
        RuntimeException b = new RuntimeException("b", a);
        a.initCause(b);

        assertThat(classifier.classify(new StepFailure.Thrown(a))).isEqualTo(TRANSIENT);
    }

    @Test
    void onlyTransientIsRetryable() {
        assertThat(TRANSIENT.isRetryable()).isTrue();
        assertThat(BUSINESS.isRetryable()).isFalse();
        assertThat(PERMANENT.isRetryable()).isFalse();
    }

    @Test
    void permanentExceptionsIncludeSpringKafkaDefaultsAndInvalidInput() {
        assertThat(classifier.permanentExceptions())
                .contains(DeserializationException.class, MessageConversionException.class, IllegalArgumentException.class);
    }

    private static SagaReply reply(StepAction action, ReplyOutcome outcome) {
        return new SagaReply(UUID.randomUUID(), UUID.randomUUID(), UUID.randomUUID(), action, outcome, "card declined");
    }
}
