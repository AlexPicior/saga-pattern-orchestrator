package com.service.orchestrator.scheduler;

import com.service.orchestrator.AbstractPostgresTest;
import com.service.orchestrator.dto.SagaCommand;
import com.service.orchestrator.entity.SagaStatus;
import com.service.orchestrator.entity.StepStatus;
import com.service.orchestrator.messaging.SagaCommandPublisher;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.bean.override.mockito.MockitoBean;

import java.util.UUID;

import static com.service.orchestrator.config.KafkaTopics.INVENTORY_COMMANDS;
import static com.service.orchestrator.config.KafkaTopics.PAYMENT_COMMANDS;
import static com.service.orchestrator.config.SagaDefinitionConfig.ORDER_SAGA;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;

class SagaReconcilerTest extends AbstractPostgresTest {

    private static final String PAYLOAD = "{}";

    @MockitoBean
    private SagaCommandPublisher publisher;

    @Autowired
    private SagaReconciler reconciler;

    @Test
    void staleSagaThatNeverStartedGetsItsFirstStep() {
        UUID sagaId = seedSaga(ORDER_SAGA, SagaStatus.STARTED, 0, PAYLOAD);

        assertThat(reconciler.sweepOnce()).isEqualTo(1);

        assertThat(readSaga(sagaId).currentStepSequence()).isEqualTo(1);
        assertThat(readStep(stepIdAt(sagaId, 1)).status()).isEqualTo(StepStatus.SENT);
        verify(publisher).publish(eq(INVENTORY_COMMANDS), any(SagaCommand.class));
    }

    @Test
    void staleSagaStuckAfterASuccessfulStepMovesOn() {
        UUID sagaId = seedSaga(ORDER_SAGA, SagaStatus.STARTED, 1, PAYLOAD);
        seedStepAt(sagaId, 1, StepStatus.SUCCESS);

        assertThat(reconciler.sweepOnce()).isEqualTo(1);

        assertThat(readSaga(sagaId).currentStepSequence()).isEqualTo(2);
        assertThat(readStep(stepIdAt(sagaId, 2)).status()).isEqualTo(StepStatus.SENT);
        verify(publisher).publish(eq(PAYMENT_COMMANDS), any(SagaCommand.class));
    }

    @Test
    void staleSagaStuckAfterAFailedStepStartsCompensation() {
        UUID sagaId = seedSaga(ORDER_SAGA, SagaStatus.STARTED, 2, PAYLOAD);
        UUID first = seedStepAt(sagaId, 1, StepStatus.SUCCESS);
        seedStepAt(sagaId, 2, StepStatus.FAILED);

        reconciler.sweepOnce();

        assertThat(readSaga(sagaId).status()).isEqualTo(SagaStatus.COMPENSATING);
        assertThat(readStep(first).status()).isEqualTo(StepStatus.COMPENSATING);
        verify(publisher).publish(eq(INVENTORY_COMMANDS), any(SagaCommand.class));
    }

    @Test
    void staleCompensatingSagaBetweenCompensationsResumesTheWalk() {
        UUID sagaId = seedSaga(ORDER_SAGA, SagaStatus.COMPENSATING, 3, PAYLOAD);
        UUID first = seedStepAt(sagaId, 1, StepStatus.SUCCESS);
        seedStepAt(sagaId, 2, StepStatus.COMPENSATED);
        seedStepAt(sagaId, 3, StepStatus.FAILED);

        assertThat(reconciler.sweepOnce()).isEqualTo(1);

        assertThat(readStep(first).status()).isEqualTo(StepStatus.COMPENSATING);
        verify(publisher).publish(eq(INVENTORY_COMMANDS), any(SagaCommand.class));
    }

    @Test
    void staleCompensatingSagaWithEverythingUndoneIsMarkedFailed() {
        UUID sagaId = seedSaga(ORDER_SAGA, SagaStatus.COMPENSATING, 2, PAYLOAD);
        seedStepAt(sagaId, 1, StepStatus.COMPENSATED);
        seedStepAt(sagaId, 2, StepStatus.FAILED);

        reconciler.sweepOnce();

        assertThat(readSaga(sagaId).status()).isEqualTo(SagaStatus.FAILED);
        verifyNoInteractions(publisher);
    }

    @Test
    void staleSagaWhoseLastStepSucceededIsCompleted() {
        UUID sagaId = seedSaga(ORDER_SAGA, SagaStatus.STARTED, 3, PAYLOAD);
        seedStepAt(sagaId, 1, StepStatus.SUCCESS);
        seedStepAt(sagaId, 2, StepStatus.SUCCESS);
        seedStepAt(sagaId, 3, StepStatus.SUCCESS);

        reconciler.sweepOnce();

        assertThat(readSaga(sagaId).status()).isEqualTo(SagaStatus.COMPLETED);
    }

    @Test
    void recentlyUpdatedSagaIsNotTouched() {
        UUID sagaId = seedSaga(ORDER_SAGA, SagaStatus.STARTED, 1, PAYLOAD);
        seedStepAt(sagaId, 1, StepStatus.SUCCESS);
        jdbc.sql("UPDATE saga_instance SET updated_at = now() WHERE saga_id = :sagaId")
                .param("sagaId", sagaId)
                .update();

        assertThat(reconciler.sweepOnce()).isZero();

        assertThat(readSaga(sagaId).currentStepSequence()).isEqualTo(1);
        verifyNoInteractions(publisher);
    }

    @ParameterizedTest
    @EnumSource(value = StepStatus.class, names = {"SENT", "COMPENSATING"})
    void sagaWithAStepInFlightIsLeftToThePoller(StepStatus inFlight) {
        SagaStatus sagaStatus = inFlight == StepStatus.SENT ? SagaStatus.STARTED : SagaStatus.COMPENSATING;
        seedStepAt(seedSaga(ORDER_SAGA, sagaStatus, 1, PAYLOAD), 1, inFlight);

        assertThat(reconciler.sweepOnce()).isZero();
        verifyNoInteractions(publisher);
    }

    @ParameterizedTest
    @EnumSource(value = SagaStatus.class, names = {"COMPLETED", "FAILED", "COMPENSATION_FAILED", "NEEDS_ATTENTION"})
    void finishedSagasAreNeverSelected(SagaStatus status) {
        seedStepAt(seedSaga(ORDER_SAGA, status, 1, PAYLOAD), 1, StepStatus.SUCCESS);

        assertThat(reconciler.sweepOnce()).isZero();
    }

    @Test
    void oneBrokenSagaDoesNotStopTheRest() {
        UUID broken = seedSaga("NO_SUCH_TYPE", SagaStatus.STARTED, 0, PAYLOAD);
        UUID sagaId = seedSaga(ORDER_SAGA, SagaStatus.STARTED, 0, PAYLOAD);

        assertThat(reconciler.sweepOnce()).isEqualTo(2);

        assertThat(countSteps(broken)).isZero();
        assertThat(readSaga(sagaId).currentStepSequence()).isEqualTo(1);
        verify(publisher).publish(eq(INVENTORY_COMMANDS), any(SagaCommand.class));
    }

    @Test
    void reconciledSagaIsNotPickedUpAgainWhileItsNewStepIsInFlight() {
        seedSaga(ORDER_SAGA, SagaStatus.STARTED, 0, PAYLOAD);
        reconciler.sweepOnce();

        assertThat(reconciler.sweepOnce()).isZero();
    }
}
