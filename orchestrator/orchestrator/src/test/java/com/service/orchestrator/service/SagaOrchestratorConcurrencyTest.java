package com.service.orchestrator.service;

import com.service.orchestrator.AbstractPostgresTest;
import com.service.orchestrator.dto.SagaCommand;
import com.service.orchestrator.entity.SagaStatus;
import com.service.orchestrator.entity.StepStatus;
import com.service.orchestrator.messaging.SagaCommandPublisher;
import com.service.orchestrator.service.SagaOrchestrator.Outcome;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.RepeatedTest;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.bean.override.mockito.MockitoBean;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static com.service.orchestrator.config.SagaDefinitionConfig.ORDER_SAGA;
import static com.service.orchestrator.entity.SagaStatus.*;
import static com.service.orchestrator.entity.StepStatus.*;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

class SagaOrchestratorConcurrencyTest extends AbstractPostgresTest {

    private static final int THREADS = 8;

    @MockitoBean
    private SagaCommandPublisher publisher;

    @Autowired
    private SagaOrchestrator orchestrator;

    private ExecutorService pool;

    @BeforeEach
    void setUp() {
        pool = Executors.newFixedThreadPool(THREADS);
    }

    @AfterEach
    void tearDown() {
        pool.shutdownNow();
    }

    @RepeatedTest(10)
    void racingAdvancesOnANewSagaCreateExactlyOneFirstStep() throws Exception {
        UUID sagaId = seedSaga(ORDER_SAGA, STARTED, 0, "{}");

        List<Outcome> outcomes = race(sagaId);

        assertThat(outcomes).filteredOn(o -> o == Outcome.STEP_DISPATCHED).hasSize(1);
        assertThat(outcomes).isSubsetOf(Outcome.STEP_DISPATCHED, Outcome.LOST_RACE, Outcome.WAITING);
        assertThat(countSteps(sagaId)).isEqualTo(1);
        assertThat(readSaga(sagaId).currentStepSequence()).isEqualTo(1);
        verify(publisher, times(1)).publish(anyString(), any(SagaCommand.class));
    }

    @RepeatedTest(10)
    void racingAdvancesAfterASuccessCreateExactlyOneNextStep() throws Exception {
        UUID sagaId = seedSaga(ORDER_SAGA, STARTED, 1, "{}");
        seedStepAt(sagaId, 1, SUCCESS);

        List<Outcome> outcomes = race(sagaId);

        assertThat(outcomes).filteredOn(o -> o == Outcome.STEP_DISPATCHED).hasSize(1);
        assertThat(outcomes).isSubsetOf(Outcome.STEP_DISPATCHED, Outcome.LOST_RACE, Outcome.WAITING);
        assertThat(countSteps(sagaId)).isEqualTo(2);
        assertThat(readSaga(sagaId).currentStepSequence()).isEqualTo(2);
        verify(publisher, times(1)).publish(anyString(), any(SagaCommand.class));
    }

    @RepeatedTest(10)
    void racingAdvancesAfterTheLastSuccessCompleteTheSagaOnce() throws Exception {
        UUID sagaId = seedSaga(ORDER_SAGA, STARTED, 3, "{}");
        seedStepAt(sagaId, 1, SUCCESS);
        seedStepAt(sagaId, 2, SUCCESS);
        seedStepAt(sagaId, 3, SUCCESS);

        List<Outcome> outcomes = race(sagaId);

        assertThat(outcomes).filteredOn(o -> o == Outcome.COMPLETED).hasSize(1);
        assertThat(outcomes).isSubsetOf(Outcome.COMPLETED, Outcome.LOST_RACE, Outcome.NOTHING_TO_DO);
        assertThat(readSaga(sagaId).status()).isEqualTo(COMPLETED);
        verify(publisher, never()).publish(anyString(), any(SagaCommand.class));
    }

    @RepeatedTest(10)
    void racingAdvancesAfterAFailureStartCompensationOnce() throws Exception {
        UUID sagaId = seedSaga(ORDER_SAGA, STARTED, 2, "{}");
        UUID first = seedStepAt(sagaId, 1, SUCCESS);
        seedStepAt(sagaId, 2, StepStatus.FAILED);

        List<Outcome> outcomes = race(sagaId);

        assertThat(outcomes).filteredOn(o -> o == Outcome.COMPENSATION_DISPATCHED).hasSize(1);
        assertThat(outcomes).isSubsetOf(Outcome.COMPENSATION_DISPATCHED, Outcome.LOST_RACE, Outcome.WAITING);
        assertThat(readSaga(sagaId).status()).isEqualTo(SagaStatus.COMPENSATING);
        assertThat(readStep(first).status()).isEqualTo(StepStatus.COMPENSATING);
        assertThat(readStep(first).attemptCount()).isEqualTo(1);
        assertThat(countSteps(sagaId)).isEqualTo(2);
        verify(publisher, times(1)).publish(anyString(), any(SagaCommand.class));
    }

    @RepeatedTest(10)
    void racingWalkersCompensateOnlyTheNewestSuccessfulStep() throws Exception {
        UUID sagaId = seedSaga(ORDER_SAGA, SagaStatus.COMPENSATING, 3, "{}");
        UUID first = seedStepAt(sagaId, 1, SUCCESS);
        UUID second = seedStepAt(sagaId, 2, SUCCESS);
        seedStepAt(sagaId, 3, StepStatus.FAILED);

        List<Outcome> outcomes = race(sagaId);

        assertThat(outcomes).filteredOn(o -> o == Outcome.COMPENSATION_DISPATCHED).hasSize(1);
        assertThat(outcomes).isSubsetOf(Outcome.COMPENSATION_DISPATCHED, Outcome.LOST_RACE, Outcome.WAITING);
        assertThat(readStep(second).status()).isEqualTo(StepStatus.COMPENSATING);
        assertThat(readStep(first).status()).isEqualTo(SUCCESS);
        verify(publisher, times(1)).publish(anyString(), any(SagaCommand.class));
    }

    @RepeatedTest(10)
    void racingWalkersFinishTheRollbackOnce() throws Exception {
        UUID sagaId = seedSaga(ORDER_SAGA, SagaStatus.COMPENSATING, 2, "{}");
        seedStepAt(sagaId, 1, COMPENSATED);
        seedStepAt(sagaId, 2, StepStatus.FAILED);

        List<Outcome> outcomes = race(sagaId);

        assertThat(outcomes).filteredOn(o -> o == Outcome.ROLLED_BACK).hasSize(1);
        assertThat(outcomes).isSubsetOf(Outcome.ROLLED_BACK, Outcome.LOST_RACE, Outcome.NOTHING_TO_DO);
        assertThat(readSaga(sagaId).status()).isEqualTo(SagaStatus.FAILED);
        verify(publisher, never()).publish(anyString(), any(SagaCommand.class));
    }

    private List<Outcome> race(UUID sagaId) throws Exception {
        CountDownLatch gate = new CountDownLatch(1);
        List<Future<Outcome>> futures = new ArrayList<>();
        for (int i = 0; i < THREADS; i++) {
            futures.add(pool.submit(() -> {
                if (!gate.await(30, TimeUnit.SECONDS)) {
                    throw new AssertionError("gate timed out");
                }
                return orchestrator.advanceSaga(sagaId);
            }));
        }
        gate.countDown();
        List<Outcome> outcomes = new ArrayList<>();
        for (Future<Outcome> future : futures) {
            outcomes.add(future.get(30, TimeUnit.SECONDS));
        }
        return outcomes;
    }
}
