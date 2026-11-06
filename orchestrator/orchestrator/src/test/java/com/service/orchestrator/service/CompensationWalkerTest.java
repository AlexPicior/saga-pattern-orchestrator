package com.service.orchestrator.service;

import com.service.orchestrator.AbstractPostgresTest;
import com.service.orchestrator.definition.SagaDefinitionRegistry;
import com.service.orchestrator.dto.ReplyOutcome;
import com.service.orchestrator.dto.SagaCommand;
import com.service.orchestrator.dto.SagaReply;
import com.service.orchestrator.dto.StepAction;
import com.service.orchestrator.entity.SagaInstance;
import com.service.orchestrator.entity.SagaStatus;
import com.service.orchestrator.entity.StepStatus;
import com.service.orchestrator.messaging.SagaCommandPublisher;
import com.service.orchestrator.repository.SagaInstanceRepository;
import com.service.orchestrator.service.SagaOrchestrator.Outcome;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.mockito.ArgumentCaptor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.transaction.IllegalTransactionStateException;

import java.util.List;
import java.util.UUID;

import static com.service.orchestrator.config.KafkaTopics.INVENTORY_COMMANDS;
import static com.service.orchestrator.config.KafkaTopics.PAYMENT_COMMANDS;
import static com.service.orchestrator.config.KafkaTopics.SHIPPING_COMMANDS;
import static com.service.orchestrator.config.SagaDefinitionConfig.ORDER_SAGA;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.contains;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;

class CompensationWalkerTest extends AbstractPostgresTest {

    private static final String PAYLOAD = """
            {"orderId":"o-1","amount":4999}
            """;

    @MockitoBean
    private SagaCommandPublisher publisher;

    @MockitoBean
    private OpsAlertService alerts;

    @Autowired
    private SagaOrchestrator orchestrator;

    @Autowired
    private ReplyHandler replies;

    @Test
    void walkStartsWithTheNewestSuccessfulStep() {
        UUID sagaId = seedSaga(ORDER_SAGA, SagaStatus.COMPENSATING, 3, PAYLOAD);
        UUID first = seedStepAt(sagaId, 1, StepStatus.SUCCESS);
        UUID second = seedStepAt(sagaId, 2, StepStatus.SUCCESS);
        seedStepAt(sagaId, 3, StepStatus.FAILED);

        assertThat(orchestrator.advanceSaga(sagaId)).isEqualTo(Outcome.COMPENSATION_DISPATCHED);

        assertThat(readStep(second).status()).isEqualTo(StepStatus.COMPENSATING);
        assertThat(readStep(first).status()).isEqualTo(StepStatus.SUCCESS);
        verify(publisher).publish(eq(PAYMENT_COMMANDS), any(SagaCommand.class));
        verify(publisher, times(1)).publish(anyString(), any(SagaCommand.class));
    }

    @Test
    void compensationCommandCarriesItsOwnIdAndTheSagaPayload() {
        UUID sagaId = seedSaga(ORDER_SAGA, SagaStatus.COMPENSATING, 2, PAYLOAD);
        UUID first = seedStepAt(sagaId, 1, StepStatus.SUCCESS);
        seedStepAt(sagaId, 2, StepStatus.FAILED);

        orchestrator.advanceSaga(sagaId);

        StepRow step = readStep(first);
        ArgumentCaptor<SagaCommand> captor = ArgumentCaptor.forClass(SagaCommand.class);
        verify(publisher).publish(eq(INVENTORY_COMMANDS), captor.capture());
        SagaCommand command = captor.getValue();
        assertThat(command.action()).isEqualTo(StepAction.COMPENSATE);
        assertThat(command.commandId()).isEqualTo(step.compensationCommandId()).isNotEqualTo(step.commandId());
        assertThat(command.attempt()).isEqualTo(1);
        assertThat(command.payload()).containsEntry("orderId", "o-1").containsEntry("amount", 4999);
        assertThat(step.attemptCount()).isEqualTo(1);
        assertThat(step.timeoutAt()).isNotNull();
    }

    @Test
    void failedAndCompensatedStepsAreSkipped() {
        UUID sagaId = seedSaga(ORDER_SAGA, SagaStatus.COMPENSATING, 3, PAYLOAD);
        UUID first = seedStepAt(sagaId, 1, StepStatus.SUCCESS);
        UUID second = seedStepAt(sagaId, 2, StepStatus.COMPENSATED);
        UUID third = seedStepAt(sagaId, 3, StepStatus.FAILED);

        orchestrator.advanceSaga(sagaId);

        assertThat(readStep(first).status()).isEqualTo(StepStatus.COMPENSATING);
        assertThat(readStep(second).status()).isEqualTo(StepStatus.COMPENSATED);
        assertThat(readStep(third).status()).isEqualTo(StepStatus.FAILED);
    }

    @Test
    void walkWaitsWhileACompensationIsInFlight() {
        UUID sagaId = seedSaga(ORDER_SAGA, SagaStatus.COMPENSATING, 3, PAYLOAD);
        UUID first = seedStepAt(sagaId, 1, StepStatus.SUCCESS);
        seedStepAt(sagaId, 2, StepStatus.COMPENSATING);
        seedStepAt(sagaId, 3, StepStatus.FAILED);

        assertThat(orchestrator.advanceSaga(sagaId)).isEqualTo(Outcome.WAITING);

        assertThat(readStep(first).status()).isEqualTo(StepStatus.SUCCESS);
        assertThat(readSaga(sagaId).status()).isEqualTo(SagaStatus.COMPENSATING);
        verifyNoInteractions(publisher);
    }

    @Test
    void nothingLeftToUndoMarksTheSagaFailed() {
        UUID sagaId = seedSaga(ORDER_SAGA, SagaStatus.COMPENSATING, 3, PAYLOAD);
        seedStepAt(sagaId, 1, StepStatus.COMPENSATED);
        seedStepAt(sagaId, 2, StepStatus.COMPENSATED);
        seedStepAt(sagaId, 3, StepStatus.FAILED);

        assertThat(orchestrator.advanceSaga(sagaId)).isEqualTo(Outcome.ROLLED_BACK);

        assertThat(readSaga(sagaId).status()).isEqualTo(SagaStatus.FAILED);
        verifyNoInteractions(publisher, alerts);
    }

    @Test
    void failedCompensationEndsTheSagaAsCompensationFailedWithoutTouchingOlderSteps() {
        UUID sagaId = seedSaga(ORDER_SAGA, SagaStatus.COMPENSATING, 3, PAYLOAD);
        UUID first = seedStepAt(sagaId, 1, StepStatus.SUCCESS);
        UUID second = seedStepAt(sagaId, 2, StepStatus.COMPENSATION_FAILED);
        seedStepAt(sagaId, 3, StepStatus.FAILED);

        assertThat(orchestrator.advanceSaga(sagaId)).isEqualTo(Outcome.COMPENSATION_FAILED);

        assertThat(readSaga(sagaId).status()).isEqualTo(SagaStatus.COMPENSATION_FAILED);
        assertThat(readStep(first).status()).isEqualTo(StepStatus.SUCCESS);
        verify(alerts).raise(eq(OpsAlertService.Type.COMPENSATION_FAILED), eq(sagaId), eq(second), any());
        verifyNoInteractions(publisher);
    }

    @Test
    void walkRefusesToCompensateASucceededPivot() {
        UUID sagaId = seedSaga(ORDER_SAGA, SagaStatus.COMPENSATING, 3, PAYLOAD);
        seedStepAt(sagaId, 1, StepStatus.SUCCESS);
        seedStepAt(sagaId, 2, StepStatus.SUCCESS);
        UUID shipping = seedStepAt(sagaId, 3, StepStatus.SUCCESS);

        assertThat(orchestrator.advanceSaga(sagaId)).isEqualTo(Outcome.ESCALATED);

        assertThat(readStep(shipping).status()).isEqualTo(StepStatus.SUCCESS);
        assertThat(readSaga(sagaId).status()).isEqualTo(SagaStatus.NEEDS_ATTENTION);
        verify(alerts).raise(eq(OpsAlertService.Type.CANNOT_COMPENSATE_PIVOT), eq(sagaId), eq(shipping),
                contains("SHIP_ORDER"));
        verifyNoInteractions(publisher);
    }

    @Test
    void failedPivotIsSkippedAndTheStepsBeforeItAreCompensated() {
        UUID sagaId = seedSaga(ORDER_SAGA, SagaStatus.STARTED, 3, PAYLOAD);
        seedStepAt(sagaId, 1, StepStatus.SUCCESS);
        UUID payment = seedStepAt(sagaId, 2, StepStatus.SUCCESS);
        seedStepAt(sagaId, 3, StepStatus.FAILED);

        assertThat(orchestrator.advanceSaga(sagaId)).isEqualTo(Outcome.COMPENSATION_DISPATCHED);

        assertThat(readStep(payment).status()).isEqualTo(StepStatus.COMPENSATING);
        verifyNoInteractions(alerts);
    }

    @ParameterizedTest
    @EnumSource(value = StepStatus.class, names = {"PENDING", "SENT", "NEEDS_ATTENTION"})
    void compensatingSagaWithAForwardStepStillOpenIsInconsistent(StepStatus status) {
        UUID sagaId = seedSaga(ORDER_SAGA, SagaStatus.COMPENSATING, 2, PAYLOAD);
        UUID first = seedStepAt(sagaId, 1, StepStatus.SUCCESS);
        seedStepAt(sagaId, 2, status);

        assertThatThrownBy(() -> orchestrator.advanceSaga(sagaId)).isInstanceOf(IllegalStateException.class);

        assertThat(readStep(first).status()).isEqualTo(StepStatus.SUCCESS);
        verifyNoInteractions(publisher);
    }

    @Test
    void walkerCannotRunOutsideATransaction(@Autowired CompensationWalker walker,
                                            @Autowired SagaInstanceRepository sagas,
                                            @Autowired SagaDefinitionRegistry definitions) {
        UUID sagaId = seedSaga(ORDER_SAGA, SagaStatus.COMPENSATING, 1, PAYLOAD);
        SagaInstance saga = sagas.findById(sagaId).orElseThrow();

        assertThatThrownBy(() -> walker.walk(saga, definitions.get(ORDER_SAGA)))
                .isInstanceOf(IllegalTransactionStateException.class);
    }

    @Test
    void shippingFailureRollsTheWholeOrderBackInReverseOrderDrivenOnlyByReplies() {
        UUID sagaId = seedSaga(ORDER_SAGA, SagaStatus.STARTED, 0, PAYLOAD);

        orchestrator.advanceSaga(sagaId);
        replies.handle(forward(sagaId, 1, ReplyOutcome.SUCCESS));
        replies.handle(forward(sagaId, 2, ReplyOutcome.SUCCESS));
        replies.handle(forward(sagaId, 3, ReplyOutcome.FAILED));
        replies.handle(compensation(sagaId, 2, ReplyOutcome.SUCCESS));
        replies.handle(compensation(sagaId, 1, ReplyOutcome.SUCCESS));

        assertThat(readSaga(sagaId).status()).isEqualTo(SagaStatus.FAILED);
        assertThat(readStep(stepIdAt(sagaId, 1)).status()).isEqualTo(StepStatus.COMPENSATED);
        assertThat(readStep(stepIdAt(sagaId, 2)).status()).isEqualTo(StepStatus.COMPENSATED);
        assertThat(readStep(stepIdAt(sagaId, 3)).status()).isEqualTo(StepStatus.FAILED);

        ArgumentCaptor<String> topics = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<SagaCommand> commands = ArgumentCaptor.forClass(SagaCommand.class);
        verify(publisher, times(5)).publish(topics.capture(), commands.capture());
        assertThat(topics.getAllValues())
                .containsExactly(INVENTORY_COMMANDS, PAYMENT_COMMANDS, SHIPPING_COMMANDS, PAYMENT_COMMANDS, INVENTORY_COMMANDS);
        List<SagaCommand> sent = commands.getAllValues();
        assertThat(sent).extracting(SagaCommand::action).containsExactly(
                StepAction.FORWARD, StepAction.FORWARD, StepAction.FORWARD, StepAction.COMPENSATE, StepAction.COMPENSATE);
        assertThat(sent).extracting(SagaCommand::commandId).doesNotHaveDuplicates();
        verifyNoInteractions(alerts);
    }

    @Test
    void duplicateCompensationReplyDoesNotDispatchTheNextCompensationTwice() {
        UUID sagaId = seedSaga(ORDER_SAGA, SagaStatus.COMPENSATING, 3, PAYLOAD);
        seedStepAt(sagaId, 1, StepStatus.SUCCESS);
        seedStepAt(sagaId, 2, StepStatus.COMPENSATING);
        seedStepAt(sagaId, 3, StepStatus.FAILED);
        SagaReply reply = compensation(sagaId, 2, ReplyOutcome.SUCCESS);

        assertThat(replies.handle(reply)).isEqualTo(ReplyHandler.Result.APPLIED);
        assertThat(replies.handle(reply)).isEqualTo(ReplyHandler.Result.DUPLICATE);

        verify(publisher, times(1)).publish(anyString(), any(SagaCommand.class));
        assertThat(readStep(stepIdAt(sagaId, 1)).attemptCount()).isEqualTo(1);
    }

    @Test
    void sagaWithAFailedCompensationIgnoresLaterAdvances() {
        UUID sagaId = seedSaga(ORDER_SAGA, SagaStatus.COMPENSATING, 2, PAYLOAD);
        seedStepAt(sagaId, 1, StepStatus.COMPENSATION_FAILED);
        seedStepAt(sagaId, 2, StepStatus.FAILED);
        orchestrator.advanceSaga(sagaId);

        assertThat(orchestrator.advanceSaga(sagaId)).isEqualTo(Outcome.NOTHING_TO_DO);
        verify(alerts, times(1)).raise(any(), any(), any(), any());
    }

    private SagaReply forward(UUID sagaId, int sequence, ReplyOutcome outcome) {
        UUID stepId = stepIdAt(sagaId, sequence);
        return new SagaReply(sagaId, stepId, readStep(stepId).commandId(), StepAction.FORWARD, outcome, null);
    }

    private SagaReply compensation(UUID sagaId, int sequence, ReplyOutcome outcome) {
        UUID stepId = stepIdAt(sagaId, sequence);
        return new SagaReply(sagaId, stepId, readStep(stepId).compensationCommandId(), StepAction.COMPENSATE, outcome, null);
    }
}
