package com.service.orchestrator.service;

import com.service.orchestrator.AbstractPostgresTest;
import com.service.orchestrator.dto.SagaCommand;
import com.service.orchestrator.dto.StepAction;
import com.service.orchestrator.entity.SagaInstance;
import com.service.orchestrator.entity.SagaStatus;
import com.service.orchestrator.entity.StepStatus;
import com.service.orchestrator.messaging.SagaCommandPublisher;
import com.service.orchestrator.repository.SagaInstanceRepository;
import com.service.orchestrator.service.SagaOrchestrator.Outcome;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.mockito.ArgumentCaptor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.UUID;

import static com.service.orchestrator.config.KafkaTopics.*;
import static com.service.orchestrator.config.SagaDefinitionConfig.ORDER_SAGA;
import static com.service.orchestrator.entity.SagaStatus.*;
import static com.service.orchestrator.entity.StepStatus.*;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

class SagaOrchestratorTest extends AbstractPostgresTest {

    private static final String PAYLOAD = """
            {"orderId":"o-1","amount":4999}
            """;

    @MockitoBean
    private SagaCommandPublisher publisher;

    @Autowired
    private SagaOrchestrator orchestrator;

    @Autowired
    private SagaInstanceRepository sagaRepository;

    @Autowired
    private PlatformTransactionManager transactionManager;

    private TransactionTemplate tx;

    @BeforeEach
    void setUp() {
        tx = new TransactionTemplate(transactionManager);
    }

    @Test
    void newSagaDispatchesTheFirstStepAndPublishesItsCommand() {
        UUID sagaId = seedSaga(ORDER_SAGA, STARTED, 0, PAYLOAD);

        assertThat(orchestrator.advanceSaga(sagaId)).isEqualTo(Outcome.STEP_DISPATCHED);

        assertThat(readSaga(sagaId).currentStepSequence()).isEqualTo(1);
        assertThat(readSaga(sagaId).status()).isEqualTo(STARTED);
        UUID stepId = stepIdAt(sagaId, 1);
        StepRow step = readStep(stepId);
        assertThat(step.status()).isEqualTo(SENT);
        assertThat(step.attemptCount()).isEqualTo(1);
        assertThat(Duration.between(step.sentAt(), step.timeoutAt())).isEqualTo(Duration.ofSeconds(30));
        assertThat(stepName(stepId)).isEqualTo("RESERVE_INVENTORY");

        SagaCommand command = publishedCommand(INVENTORY_COMMANDS);
        assertThat(command.sagaId()).isEqualTo(sagaId);
        assertThat(command.stepId()).isEqualTo(stepId);
        assertThat(command.commandId()).isEqualTo(step.commandId());
        assertThat(command.action()).isEqualTo(StepAction.FORWARD);
        assertThat(command.attempt()).isEqualTo(1);
        assertThat(command.payload()).containsEntry("orderId", "o-1").containsEntry("amount", 4999);
    }

    @Test
    void successfulStepDispatchesTheNextOne() {
        UUID sagaId = seedSaga(ORDER_SAGA, STARTED, 1, PAYLOAD);
        seedStepAt(sagaId, 1, SUCCESS);

        assertThat(orchestrator.advanceSaga(sagaId)).isEqualTo(Outcome.STEP_DISPATCHED);

        assertThat(readSaga(sagaId).currentStepSequence()).isEqualTo(2);
        UUID stepId = stepIdAt(sagaId, 2);
        assertThat(readStep(stepId).status()).isEqualTo(SENT);
        assertThat(stepName(stepId)).isEqualTo("PROCESS_PAYMENT");
        assertThat(publishedCommand(PAYMENT_COMMANDS).stepId()).isEqualTo(stepId);
    }

    @Test
    void successOfTheLastStepCompletesTheSaga() {
        UUID sagaId = seedSaga(ORDER_SAGA, STARTED, 3, PAYLOAD);
        seedStepAt(sagaId, 1, SUCCESS);
        seedStepAt(sagaId, 2, SUCCESS);
        seedStepAt(sagaId, 3, SUCCESS);

        assertThat(orchestrator.advanceSaga(sagaId)).isEqualTo(Outcome.COMPLETED);

        assertThat(readSaga(sagaId).status()).isEqualTo(COMPLETED);
        assertThat(countSteps(sagaId)).isEqualTo(3);
        verifyNoInteractions(publisher);
    }

    @Test
    void failedStepStartsCompensationOfThePreviousStepInTheSameTransaction() {
        UUID sagaId = seedSaga(ORDER_SAGA, STARTED, 2, PAYLOAD);
        UUID first = seedStepAt(sagaId, 1, SUCCESS);
        seedStepAt(sagaId, 2, StepStatus.FAILED);

        assertThat(orchestrator.advanceSaga(sagaId)).isEqualTo(Outcome.COMPENSATION_DISPATCHED);

        assertThat(readSaga(sagaId).status()).isEqualTo(SagaStatus.COMPENSATING);
        assertThat(readSaga(sagaId).currentStepSequence()).isEqualTo(2);
        assertThat(countSteps(sagaId)).isEqualTo(2);
        assertThat(readStep(first).status()).isEqualTo(StepStatus.COMPENSATING);
        SagaCommand command = publishedCommand(INVENTORY_COMMANDS);
        assertThat(command.action()).isEqualTo(StepAction.COMPENSATE);
        assertThat(command.commandId()).isEqualTo(readStep(first).compensationCommandId());
    }

    @Test
    void failedFirstStepHasNothingToCompensateAndRollsBackAtOnce() {
        UUID sagaId = seedSaga(ORDER_SAGA, STARTED, 1, PAYLOAD);
        seedStepAt(sagaId, 1, StepStatus.FAILED);

        assertThat(orchestrator.advanceSaga(sagaId)).isEqualTo(Outcome.ROLLED_BACK);

        assertThat(readSaga(sagaId).status()).isEqualTo(SagaStatus.FAILED);
        verifyNoInteractions(publisher);
    }

    @Test
    void stepStillInFlightMeansWaiting() {
        UUID sagaId = seedSaga(ORDER_SAGA, STARTED, 1, PAYLOAD);
        seedStepAt(sagaId, 1, SENT);
        SagaRow before = readSaga(sagaId);

        assertThat(orchestrator.advanceSaga(sagaId)).isEqualTo(Outcome.WAITING);

        assertThat(readSaga(sagaId)).isEqualTo(before);
        assertThat(countSteps(sagaId)).isEqualTo(1);
        verifyNoInteractions(publisher);
    }

    @ParameterizedTest
    @EnumSource(value = SagaStatus.class, names = {"COMPLETED", "FAILED", "COMPENSATION_FAILED", "NEEDS_ATTENTION"})
    void finishedSagaIsLeftAlone(SagaStatus status) {
        UUID sagaId = seedSaga(ORDER_SAGA, status, 1, PAYLOAD);
        seedStepAt(sagaId, 1, SUCCESS);
        SagaRow before = readSaga(sagaId);

        assertThat(orchestrator.advanceSaga(sagaId)).isEqualTo(Outcome.NOTHING_TO_DO);

        assertThat(readSaga(sagaId)).isEqualTo(before);
        verifyNoInteractions(publisher);
    }

    @Test
    void unknownSagaIsRejected() {
        assertThatThrownBy(() -> orchestrator.advanceSaga(UUID.randomUUID()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Unknown saga");
    }

    @Test
    void sagaOfUnknownTypeIsRejected() {
        UUID sagaId = seedSaga("NO_SUCH_TYPE", STARTED, 0, PAYLOAD);

        assertThatThrownBy(() -> orchestrator.advanceSaga(sagaId))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("NO_SUCH_TYPE");
        assertThat(countSteps(sagaId)).isZero();
    }

    @ParameterizedTest
    @EnumSource(value = StepStatus.class, names = {"COMPENSATING", "NEEDS_ATTENTION"})
    void startedSagaPointingAtAStepItCannotAdvanceFromIsInconsistent(StepStatus stepStatus) {
        UUID sagaId = seedSaga(ORDER_SAGA, STARTED, 1, PAYLOAD);
        seedStepAt(sagaId, 1, stepStatus);

        assertThatThrownBy(() -> orchestrator.advanceSaga(sagaId)).isInstanceOf(IllegalStateException.class);
    }

    @Test
    void commandIsPublishedOnlyAfterTheTransactionCommits() {
        UUID sagaId = seedSaga(ORDER_SAGA, STARTED, 0, PAYLOAD);

        tx.executeWithoutResult(status -> {
            assertThat(orchestrator.advanceSaga(sagaId)).isEqualTo(Outcome.STEP_DISPATCHED);
            verifyNoInteractions(publisher);
        });

        verify(publisher).publish(eq(INVENTORY_COMMANDS), any(SagaCommand.class));
    }

    @Test
    void rolledBackAdvanceLeavesNoStepAndPublishesNothing() {
        UUID sagaId = seedSaga(ORDER_SAGA, STARTED, 0, PAYLOAD);

        tx.executeWithoutResult(status -> {
            orchestrator.advanceSaga(sagaId);
            status.setRollbackOnly();
        });

        assertThat(readSaga(sagaId).currentStepSequence()).isZero();
        assertThat(countSteps(sagaId)).isZero();
        verifyNoInteractions(publisher);
    }

    @Test
    void failedPublishDoesNotUndoTheCommittedStep() {
        when(publisher.publish(anyString(), any())).thenThrow(new RuntimeException("broker down"));
        UUID sagaId = seedSaga(ORDER_SAGA, STARTED, 0, PAYLOAD);

        assertThat(orchestrator.advanceSaga(sagaId)).isEqualTo(Outcome.STEP_DISPATCHED);

        StepRow step = readStep(stepIdAt(sagaId, 1));
        assertThat(step.status()).isEqualTo(SENT);
        assertThat(step.timeoutAt()).isNotNull();
    }

    @Test
    void sagaCreatedThroughTheEntityCarriesItsPayloadIntoTheCommand() {
        SagaInstance saga = sagaRepository.saveAndFlush(
                SagaInstance.start(ORDER_SAGA, Map.of("orderId", "o-7", "items", 3), Instant.now()));

        orchestrator.advanceSaga(saga.getSagaId());

        assertThat(publishedCommand(INVENTORY_COMMANDS).payload())
                .containsEntry("orderId", "o-7")
                .containsEntry("items", 3);
    }

    private SagaCommand publishedCommand(String topic) {
        ArgumentCaptor<SagaCommand> captor = ArgumentCaptor.forClass(SagaCommand.class);
        verify(publisher).publish(eq(topic), captor.capture());
        return captor.getValue();
    }

    private String stepName(UUID stepId) {
        return jdbc.sql("SELECT step_name FROM saga_step_log WHERE step_id = :stepId")
                .param("stepId", stepId)
                .query(String.class)
                .single();
    }
}
