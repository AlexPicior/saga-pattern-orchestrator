package com.service.orchestrator.service;

import com.service.orchestrator.AbstractPostgresTest;
import com.service.orchestrator.dto.ReplyOutcome;
import com.service.orchestrator.dto.SagaCommand;
import com.service.orchestrator.dto.SagaReply;
import com.service.orchestrator.dto.StepAction;
import com.service.orchestrator.entity.SagaStatus;
import com.service.orchestrator.entity.StepStatus;
import com.service.orchestrator.messaging.SagaCommandPublisher;
import com.service.orchestrator.service.ReplyHandler.Result;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.bean.override.mockito.MockitoBean;

import java.util.UUID;

import static com.service.orchestrator.config.KafkaTopics.INVENTORY_COMMANDS;
import static com.service.orchestrator.config.KafkaTopics.PAYMENT_COMMANDS;
import static com.service.orchestrator.config.SagaDefinitionConfig.ORDER_SAGA;
import static com.service.orchestrator.dto.ReplyOutcome.FAILED;
import static com.service.orchestrator.dto.ReplyOutcome.SUCCESS;
import static com.service.orchestrator.dto.StepAction.COMPENSATE;
import static com.service.orchestrator.dto.StepAction.FORWARD;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.contains;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;

class ReplyHandlerTest extends AbstractPostgresTest {

    private static final String PAYLOAD = """
            {"orderId":"o-1"}
            """;

    @MockitoBean
    private SagaCommandPublisher publisher;

    @MockitoBean
    private OpsAlertService alerts;

    @Autowired
    private ReplyHandler handler;

    @Test
    void forwardSuccessCompletesTheStepAndDispatchesTheNextOne() {
        UUID sagaId = seedSaga(ORDER_SAGA, SagaStatus.STARTED, 1, PAYLOAD);
        UUID stepId = seedStepAt(sagaId, 1, StepStatus.SENT);

        Result result = handler.handle(forwardReply(sagaId, stepId, SUCCESS));

        assertThat(result).isEqualTo(Result.APPLIED);
        assertThat(readStep(stepId).status()).isEqualTo(StepStatus.SUCCESS);
        assertThat(readSaga(sagaId).currentStepSequence()).isEqualTo(2);
        assertThat(readStep(stepIdAt(sagaId, 2)).status()).isEqualTo(StepStatus.SENT);
        verify(publisher).publish(eq(PAYMENT_COMMANDS), any(SagaCommand.class));
        verifyNoInteractions(alerts);
    }

    @Test
    void forwardSuccessOnTheLastStepCompletesTheSaga() {
        UUID sagaId = seedSaga(ORDER_SAGA, SagaStatus.STARTED, 3, PAYLOAD);
        seedStepAt(sagaId, 1, StepStatus.SUCCESS);
        seedStepAt(sagaId, 2, StepStatus.SUCCESS);
        UUID stepId = seedStepAt(sagaId, 3, StepStatus.SENT);

        assertThat(handler.handle(forwardReply(sagaId, stepId, SUCCESS))).isEqualTo(Result.APPLIED);

        assertThat(readSaga(sagaId).status()).isEqualTo(SagaStatus.COMPLETED);
        verifyNoInteractions(publisher);
    }

    @Test
    void forwardFailureFailsTheStepAndStartsCompensation() {
        UUID sagaId = seedSaga(ORDER_SAGA, SagaStatus.STARTED, 2, PAYLOAD);
        UUID first = seedStepAt(sagaId, 1, StepStatus.SUCCESS);
        UUID stepId = seedStepAt(sagaId, 2, StepStatus.SENT);

        assertThat(handler.handle(forwardReply(sagaId, stepId, FAILED))).isEqualTo(Result.APPLIED);

        assertThat(readStep(stepId).status()).isEqualTo(StepStatus.FAILED);
        assertThat(readSaga(sagaId).status()).isEqualTo(SagaStatus.COMPENSATING);
        assertThat(readStep(first).status()).isEqualTo(StepStatus.COMPENSATING);
        verify(publisher).publish(eq(INVENTORY_COMMANDS), any(SagaCommand.class));
        verifyNoInteractions(alerts);
    }

    @Test
    void duplicateForwardSuccessIsDroppedQuietly() {
        UUID sagaId = seedSaga(ORDER_SAGA, SagaStatus.STARTED, 1, PAYLOAD);
        UUID stepId = seedStepAt(sagaId, 1, StepStatus.SENT);
        SagaReply reply = forwardReply(sagaId, stepId, SUCCESS);
        handler.handle(reply);

        assertThat(handler.handle(reply)).isEqualTo(Result.DUPLICATE);

        assertThat(countSteps(sagaId)).isEqualTo(2);
        verify(publisher).publish(anyString(), any(SagaCommand.class));
        verifyNoInteractions(alerts);
    }

    @ParameterizedTest
    @EnumSource(value = StepStatus.class, names = {"COMPENSATING", "COMPENSATED", "COMPENSATION_FAILED"})
    void staleForwardSuccessNeverCompletesACompensation(StepStatus laterStatus) {
        UUID sagaId = seedSaga(ORDER_SAGA, SagaStatus.COMPENSATING, 1, PAYLOAD);
        UUID stepId = seedStepAt(sagaId, 1, laterStatus);
        StepRow before = readStep(stepId);

        assertThat(handler.handle(forwardReply(sagaId, stepId, SUCCESS))).isEqualTo(Result.DUPLICATE);

        assertThat(readStep(stepId)).isEqualTo(before);
        verifyNoInteractions(alerts);
    }

    @Test
    void lateSuccessForAStepAlreadyMarkedFailedRaisesAnAlert() {
        UUID sagaId = seedSaga(ORDER_SAGA, SagaStatus.COMPENSATING, 2, PAYLOAD);
        seedStepAt(sagaId, 1, StepStatus.SUCCESS);
        UUID stepId = seedStepAt(sagaId, 2, StepStatus.FAILED);

        assertThat(handler.handle(forwardReply(sagaId, stepId, SUCCESS))).isEqualTo(Result.CONTRADICTION);

        assertThat(readStep(stepId).status()).isEqualTo(StepStatus.FAILED);
        verify(alerts).raise(eq(OpsAlertService.Type.REPLY_CONTRADICTS_LOG), eq(sagaId), eq(stepId), contains("FAILED"));
    }

    @Test
    void failureReplyForAStepThatSucceededRaisesAnAlert() {
        UUID sagaId = seedSaga(ORDER_SAGA, SagaStatus.STARTED, 1, PAYLOAD);
        UUID stepId = seedStepAt(sagaId, 1, StepStatus.SUCCESS);

        assertThat(handler.handle(forwardReply(sagaId, stepId, FAILED))).isEqualTo(Result.CONTRADICTION);

        assertThat(readStep(stepId).status()).isEqualTo(StepStatus.SUCCESS);
        verify(alerts).raise(eq(OpsAlertService.Type.REPLY_CONTRADICTS_LOG), eq(sagaId), eq(stepId), any());
    }

    @Test
    void lateSuccessForAnEscalatedStepRaisesAnAlertAndLeavesItForOps() {
        UUID sagaId = seedSaga(ORDER_SAGA, SagaStatus.NEEDS_ATTENTION, 1, PAYLOAD);
        UUID stepId = seedStepAt(sagaId, 1, StepStatus.NEEDS_ATTENTION);

        assertThat(handler.handle(forwardReply(sagaId, stepId, SUCCESS))).isEqualTo(Result.CONTRADICTION);

        assertThat(readStep(stepId).status()).isEqualTo(StepStatus.NEEDS_ATTENTION);
        assertThat(readSaga(sagaId).status()).isEqualTo(SagaStatus.NEEDS_ATTENTION);
        verify(alerts).raise(eq(OpsAlertService.Type.REPLY_CONTRADICTS_LOG), eq(sagaId), eq(stepId),
                contains("NEEDS_ATTENTION"));
        verifyNoInteractions(publisher);
    }

    @Test
    void compensationSuccessOfTheLastStepToUndoRollsTheSagaBack() {
        UUID sagaId = seedSaga(ORDER_SAGA, SagaStatus.COMPENSATING, 2, PAYLOAD);
        UUID stepId = seedStepAt(sagaId, 1, StepStatus.COMPENSATING);
        seedStepAt(sagaId, 2, StepStatus.FAILED);

        assertThat(handler.handle(compensationReply(sagaId, stepId, SUCCESS))).isEqualTo(Result.APPLIED);

        assertThat(readStep(stepId).status()).isEqualTo(StepStatus.COMPENSATED);
        assertThat(readSaga(sagaId).status()).isEqualTo(SagaStatus.FAILED);
        verifyNoInteractions(alerts, publisher);
    }

    @Test
    void compensationSuccessMovesTheWalkToTheNextOlderStep() {
        UUID sagaId = seedSaga(ORDER_SAGA, SagaStatus.COMPENSATING, 3, PAYLOAD);
        UUID first = seedStepAt(sagaId, 1, StepStatus.SUCCESS);
        UUID second = seedStepAt(sagaId, 2, StepStatus.COMPENSATING);
        seedStepAt(sagaId, 3, StepStatus.FAILED);

        assertThat(handler.handle(compensationReply(sagaId, second, SUCCESS))).isEqualTo(Result.APPLIED);

        assertThat(readStep(second).status()).isEqualTo(StepStatus.COMPENSATED);
        assertThat(readStep(first).status()).isEqualTo(StepStatus.COMPENSATING);
        assertThat(readSaga(sagaId).status()).isEqualTo(SagaStatus.COMPENSATING);
        verify(publisher).publish(eq(INVENTORY_COMMANDS), any(SagaCommand.class));
    }

    @Test
    void compensationFailureStopsTheWalkAndAlerts() {
        UUID sagaId = seedSaga(ORDER_SAGA, SagaStatus.COMPENSATING, 3, PAYLOAD);
        UUID first = seedStepAt(sagaId, 1, StepStatus.SUCCESS);
        UUID stepId = seedStepAt(sagaId, 2, StepStatus.COMPENSATING);
        seedStepAt(sagaId, 3, StepStatus.FAILED);

        assertThat(handler.handle(compensationReply(sagaId, stepId, FAILED))).isEqualTo(Result.APPLIED);

        assertThat(readStep(stepId).status()).isEqualTo(StepStatus.COMPENSATION_FAILED);
        assertThat(readSaga(sagaId).status()).isEqualTo(SagaStatus.COMPENSATION_FAILED);
        assertThat(readStep(first).status()).isEqualTo(StepStatus.SUCCESS);
        verify(alerts).raise(eq(OpsAlertService.Type.COMPENSATION_FAILED), eq(sagaId), eq(stepId), any());
        verifyNoInteractions(publisher);
    }

    @Test
    void duplicateCompensationSuccessIsDroppedQuietly() {
        UUID sagaId = seedSaga(ORDER_SAGA, SagaStatus.COMPENSATING, 2, PAYLOAD);
        UUID stepId = seedStepAt(sagaId, 1, StepStatus.COMPENSATING);
        seedStepAt(sagaId, 2, StepStatus.FAILED);
        SagaReply reply = compensationReply(sagaId, stepId, SUCCESS);
        handler.handle(reply);

        assertThat(handler.handle(reply)).isEqualTo(Result.DUPLICATE);
        verifyNoInteractions(alerts);
    }

    @Test
    void compensationReplyCarryingTheForwardCommandIdIsRejected() {
        UUID sagaId = seedSaga(ORDER_SAGA, SagaStatus.COMPENSATING, 1, PAYLOAD);
        UUID stepId = seedStepAt(sagaId, 1, StepStatus.COMPENSATING);
        StepRow before = readStep(stepId);
        SagaReply reply = new SagaReply(sagaId, stepId, before.commandId(), COMPENSATE, SUCCESS, null);

        assertThatThrownBy(() -> handler.handle(reply))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("COMPENSATE command");
        assertThat(readStep(stepId)).isEqualTo(before);
    }

    @Test
    void forwardReplyWithAnUnknownCommandIdIsRejected() {
        UUID sagaId = seedSaga(ORDER_SAGA, SagaStatus.STARTED, 1, PAYLOAD);
        UUID stepId = seedStepAt(sagaId, 1, StepStatus.SENT);
        SagaReply reply = new SagaReply(sagaId, stepId, UUID.randomUUID(), FORWARD, SUCCESS, null);

        assertThatThrownBy(() -> handler.handle(reply)).isInstanceOf(IllegalArgumentException.class);
        assertThat(readStep(stepId).status()).isEqualTo(StepStatus.SENT);
    }

    @Test
    void replyForAnUnknownStepIsRejected() {
        SagaReply reply = new SagaReply(UUID.randomUUID(), UUID.randomUUID(), UUID.randomUUID(), FORWARD, SUCCESS, null);

        assertThatThrownBy(() -> handler.handle(reply))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("unknown step");
    }

    @Test
    void replyNamingTheWrongSagaIsRejected() {
        UUID sagaId = seedSaga(ORDER_SAGA, SagaStatus.STARTED, 1, PAYLOAD);
        UUID stepId = seedStepAt(sagaId, 1, StepStatus.SENT);
        SagaReply reply = new SagaReply(UUID.randomUUID(), stepId, readStep(stepId).commandId(), FORWARD, SUCCESS, null);

        assertThatThrownBy(() -> handler.handle(reply)).isInstanceOf(IllegalArgumentException.class);
        assertThat(readStep(stepId).status()).isEqualTo(StepStatus.SENT);
    }

    @Test
    void failureWhileAdvancingRollsBackTheStepChangeToo() {
        UUID sagaId = seedSaga("NO_SUCH_TYPE", SagaStatus.STARTED, 1, PAYLOAD);
        UUID stepId = seedStepAt(sagaId, 1, StepStatus.SENT);

        assertThatThrownBy(() -> handler.handle(forwardReply(sagaId, stepId, SUCCESS)))
                .isInstanceOf(IllegalArgumentException.class);

        assertThat(readStep(stepId).status()).isEqualTo(StepStatus.SENT);
        verifyNoInteractions(publisher);
    }

    private SagaReply forwardReply(UUID sagaId, UUID stepId, ReplyOutcome outcome) {
        return new SagaReply(sagaId, stepId, readStep(stepId).commandId(), FORWARD, outcome, null);
    }

    private SagaReply compensationReply(UUID sagaId, UUID stepId, ReplyOutcome outcome) {
        return new SagaReply(sagaId, stepId, readStep(stepId).compensationCommandId(), COMPENSATE, outcome, null);
    }
}
