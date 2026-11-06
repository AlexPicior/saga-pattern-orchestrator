package com.service.orchestrator.scheduler;

import com.service.orchestrator.AbstractPostgresTest;
import com.service.orchestrator.dto.SagaCommand;
import com.service.orchestrator.dto.StepAction;
import com.service.orchestrator.entity.SagaStatus;
import com.service.orchestrator.entity.StepStatus;
import com.service.orchestrator.messaging.SagaCommandPublisher;
import com.service.orchestrator.repository.StepLogReader.OverdueStep;
import com.service.orchestrator.service.OpsAlertService;
import com.service.orchestrator.service.StepTimeoutHandler;
import com.service.orchestrator.service.StepTimeoutHandler.Result;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.bean.override.mockito.MockitoBean;

import java.time.OffsetDateTime;
import java.util.UUID;

import static com.service.orchestrator.config.KafkaTopics.INVENTORY_COMMANDS;
import static com.service.orchestrator.config.KafkaTopics.PAYMENT_COMMANDS;
import static com.service.orchestrator.config.SagaDefinitionConfig.ORDER_SAGA;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.contains;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;

class OverdueStepPollerTest extends AbstractPostgresTest {

    private static final String PAYLOAD = """
            {"orderId":"o-1"}
            """;

    @MockitoBean
    private SagaCommandPublisher publisher;

    @MockitoBean
    private OpsAlertService alerts;

    @Autowired
    private OverdueStepPoller poller;

    @Autowired
    private StepTimeoutHandler handler;

    @Test
    void overdueForwardStepIsResentUnderTheSameCommandId() {
        UUID sagaId = seedSaga(ORDER_SAGA, SagaStatus.STARTED, 1, PAYLOAD);
        UUID stepId = seedStepAt(sagaId, 1, StepStatus.SENT);
        makeOverdue(stepId, 1);
        StepRow before = readStep(stepId);

        assertThat(poller.pollOnce()).isEqualTo(1);

        StepRow after = readStep(stepId);
        assertThat(after.status()).isEqualTo(StepStatus.SENT);
        assertThat(after.attemptCount()).isEqualTo(2);
        assertThat(after.commandId()).isEqualTo(before.commandId());
        assertThat(after.timeoutAt()).isAfter(OffsetDateTime.now());

        SagaCommand command = publishedCommand(INVENTORY_COMMANDS);
        assertThat(command.stepId()).isEqualTo(stepId);
        assertThat(command.commandId()).isEqualTo(before.commandId());
        assertThat(command.action()).isEqualTo(StepAction.FORWARD);
        assertThat(command.attempt()).isEqualTo(2);
        assertThat(command.payload()).containsEntry("orderId", "o-1");
        verifyNoInteractions(alerts);
    }

    @Test
    void overdueCompensationIsResentUnderTheCompensationCommandId() {
        UUID sagaId = seedSaga(ORDER_SAGA, SagaStatus.COMPENSATING, 2, PAYLOAD);
        UUID stepId = seedStepAt(sagaId, 1, StepStatus.COMPENSATING);
        seedStepAt(sagaId, 2, StepStatus.FAILED);
        makeOverdue(stepId, 1);
        StepRow before = readStep(stepId);

        assertThat(poller.pollOnce()).isEqualTo(1);

        assertThat(readStep(stepId).status()).isEqualTo(StepStatus.COMPENSATING);
        assertThat(readStep(stepId).attemptCount()).isEqualTo(2);
        SagaCommand command = publishedCommand(INVENTORY_COMMANDS);
        assertThat(command.action()).isEqualTo(StepAction.COMPENSATE);
        assertThat(command.commandId()).isEqualTo(before.compensationCommandId());
        assertThat(command.commandId()).isNotEqualTo(before.commandId());
    }

    @Test
    void resendUsesTheTopicOfTheStepsOwnSequence() {
        UUID sagaId = seedSaga(ORDER_SAGA, SagaStatus.STARTED, 2, PAYLOAD);
        seedStepAt(sagaId, 1, StepStatus.SUCCESS);
        UUID stepId = seedStepAt(sagaId, 2, StepStatus.SENT);
        makeOverdue(stepId, 1);

        poller.pollOnce();

        assertThat(publishedCommand(PAYMENT_COMMANDS).stepId()).isEqualTo(stepId);
    }

    @Test
    void stepsThatAreNotOverdueOrNotInFlightAreLeftAlone() {
        UUID sagaId = seedSaga(ORDER_SAGA, SagaStatus.STARTED, 3, PAYLOAD);
        UUID answered = seedStepAt(sagaId, 1, StepStatus.SUCCESS);
        makeOverdue(answered, 1);
        UUID notDue = seedStepAt(sagaId, 2, StepStatus.SENT);
        makeNotDueYet(notDue);
        UUID neverArmed = seedStepAt(sagaId, 3, StepStatus.SENT);
        StepRow answeredBefore = readStep(answered);
        StepRow notDueBefore = readStep(notDue);
        StepRow neverArmedBefore = readStep(neverArmed);

        assertThat(poller.pollOnce()).isZero();

        assertThat(readStep(answered)).isEqualTo(answeredBefore);
        assertThat(readStep(notDue)).isEqualTo(notDueBefore);
        assertThat(readStep(neverArmed)).isEqualTo(neverArmedBefore);
        verifyNoInteractions(publisher, alerts);
    }

    @Test
    void lastAllowedAttemptIsStillResent() {
        UUID sagaId = seedSaga(ORDER_SAGA, SagaStatus.STARTED, 1, PAYLOAD);
        UUID stepId = seedStepAt(sagaId, 1, StepStatus.SENT);
        makeOverdue(stepId, 4);

        poller.pollOnce();

        assertThat(readStep(stepId).status()).isEqualTo(StepStatus.SENT);
        assertThat(readStep(stepId).attemptCount()).isEqualTo(5);
        assertThat(publishedCommand(INVENTORY_COMMANDS).attempt()).isEqualTo(5);
        verifyNoInteractions(alerts);
    }

    @Test
    void forwardStepOutOfAttemptsIsEscalatedAndNeverMarkedFailed() {
        UUID sagaId = seedSaga(ORDER_SAGA, SagaStatus.STARTED, 1, PAYLOAD);
        UUID stepId = seedStepAt(sagaId, 1, StepStatus.SENT);
        makeOverdue(stepId, 5);

        assertThat(poller.pollOnce()).isEqualTo(1);

        assertThat(readStep(stepId).status()).isEqualTo(StepStatus.NEEDS_ATTENTION);
        assertThat(readStep(stepId).attemptCount()).isEqualTo(5);
        assertThat(readSaga(sagaId).status()).isEqualTo(SagaStatus.NEEDS_ATTENTION);
        assertThat(countSteps(sagaId)).isEqualTo(1);
        verify(alerts).raise(eq(OpsAlertService.Type.STEP_NEEDS_ATTENTION), eq(sagaId), eq(stepId), contains("5 attempts"));
        verifyNoInteractions(publisher);
    }

    @Test
    void compensationOutOfAttemptsEscalatesTheCompensatingSaga() {
        UUID sagaId = seedSaga(ORDER_SAGA, SagaStatus.COMPENSATING, 2, PAYLOAD);
        UUID stepId = seedStepAt(sagaId, 1, StepStatus.COMPENSATING);
        seedStepAt(sagaId, 2, StepStatus.FAILED);
        makeOverdue(stepId, 5);

        poller.pollOnce();

        assertThat(readStep(stepId).status()).isEqualTo(StepStatus.NEEDS_ATTENTION);
        assertThat(readSaga(sagaId).status()).isEqualTo(SagaStatus.NEEDS_ATTENTION);
        verify(alerts).raise(eq(OpsAlertService.Type.STEP_NEEDS_ATTENTION), eq(sagaId), eq(stepId), contains("COMPENSATING"));
        verifyNoInteractions(publisher);
    }

    @Test
    void escalatedStepIsNotPickedUpAgain() {
        UUID sagaId = seedSaga(ORDER_SAGA, SagaStatus.STARTED, 1, PAYLOAD);
        UUID stepId = seedStepAt(sagaId, 1, StepStatus.SENT);
        makeOverdue(stepId, 5);
        poller.pollOnce();

        assertThat(poller.pollOnce()).isZero();
    }

    @Test
    void resendLosesWhenTheStepWasResentSinceItWasRead() {
        UUID sagaId = seedSaga(ORDER_SAGA, SagaStatus.STARTED, 1, PAYLOAD);
        UUID stepId = seedStepAt(sagaId, 1, StepStatus.SENT);
        makeOverdue(stepId, 2);
        StepRow before = readStep(stepId);

        assertThat(handler.handle(snapshot(sagaId, stepId, StepStatus.SENT, 1))).isEqualTo(Result.LOST_RACE);

        assertThat(readStep(stepId)).isEqualTo(before);
        verifyNoInteractions(publisher);
    }

    @Test
    void escalationLosesWhenTheReplyArrivedSinceTheStepWasRead() {
        UUID sagaId = seedSaga(ORDER_SAGA, SagaStatus.STARTED, 1, PAYLOAD);
        UUID stepId = seedStepAt(sagaId, 1, StepStatus.SUCCESS);
        SagaRow sagaBefore = readSaga(sagaId);

        assertThat(handler.handle(snapshot(sagaId, stepId, StepStatus.SENT, 5))).isEqualTo(Result.LOST_RACE);

        assertThat(readStep(stepId).status()).isEqualTo(StepStatus.SUCCESS);
        assertThat(readSaga(sagaId)).isEqualTo(sagaBefore);
        verifyNoInteractions(publisher, alerts);
    }

    @Test
    void escalationOfAStepWhoseSagaIsInAnotherStateRollsBackTheStepToo() {
        UUID sagaId = seedSaga(ORDER_SAGA, SagaStatus.COMPENSATING, 1, PAYLOAD);
        UUID stepId = seedStepAt(sagaId, 1, StepStatus.SENT);
        makeOverdue(stepId, 5);

        assertThatThrownBy(() -> handler.handle(snapshot(sagaId, stepId, StepStatus.SENT, 5)))
                .isInstanceOf(IllegalStateException.class);

        assertThat(readStep(stepId).status()).isEqualTo(StepStatus.SENT);
        assertThat(readSaga(sagaId).status()).isEqualTo(SagaStatus.COMPENSATING);
        verifyNoInteractions(alerts);
    }

    @Test
    void oneBrokenStepDoesNotStopTheRestOfTheBatch() {
        UUID brokenSaga = seedSaga("NO_SUCH_TYPE", SagaStatus.STARTED, 1, PAYLOAD);
        UUID brokenStep = seedStepAt(brokenSaga, 1, StepStatus.SENT);
        makeOverdue(brokenStep, 1);
        UUID sagaId = seedSaga(ORDER_SAGA, SagaStatus.STARTED, 1, PAYLOAD);
        UUID stepId = seedStepAt(sagaId, 1, StepStatus.SENT);
        makeOverdue(stepId, 1);

        assertThat(poller.pollOnce()).isEqualTo(2);

        assertThat(readStep(brokenStep).attemptCount()).isEqualTo(1);
        assertThat(readStep(stepId).attemptCount()).isEqualTo(2);
        verify(publisher).publish(eq(INVENTORY_COMMANDS), any(SagaCommand.class));
    }

    private OverdueStep snapshot(UUID sagaId, UUID stepId, StepStatus status, int attemptCount) {
        StepRow row = readStep(stepId);
        return new OverdueStep(stepId, sagaId, 1, status, attemptCount, row.commandId(), row.compensationCommandId());
    }

    private SagaCommand publishedCommand(String topic) {
        ArgumentCaptor<SagaCommand> captor = ArgumentCaptor.forClass(SagaCommand.class);
        verify(publisher).publish(eq(topic), captor.capture());
        verify(publisher).publish(anyString(), any(SagaCommand.class));
        return captor.getValue();
    }
}
