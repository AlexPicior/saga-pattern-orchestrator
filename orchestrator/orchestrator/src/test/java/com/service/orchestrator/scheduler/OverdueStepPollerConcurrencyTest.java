package com.service.orchestrator.scheduler;

import com.service.orchestrator.AbstractPostgresTest;
import com.service.orchestrator.dto.ReplyOutcome;
import com.service.orchestrator.dto.SagaCommand;
import com.service.orchestrator.dto.SagaReply;
import com.service.orchestrator.dto.StepAction;
import com.service.orchestrator.entity.SagaStatus;
import com.service.orchestrator.entity.StepStatus;
import com.service.orchestrator.messaging.SagaCommandPublisher;
import com.service.orchestrator.repository.StepLogReader.OverdueStep;
import com.service.orchestrator.service.OpsAlertService;
import com.service.orchestrator.service.ReplyHandler;
import com.service.orchestrator.service.StepTimeoutHandler;
import com.service.orchestrator.service.StepTimeoutHandler.Result;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.RepeatedTest;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.bean.override.mockito.MockitoBean;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static com.service.orchestrator.config.SagaDefinitionConfig.ORDER_SAGA;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;

class OverdueStepPollerConcurrencyTest extends AbstractPostgresTest {

    private static final int THREADS = 8;

    @MockitoBean
    private SagaCommandPublisher publisher;

    @MockitoBean
    private OpsAlertService alerts;

    @Autowired
    private StepTimeoutHandler timeouts;

    @Autowired
    private ReplyHandler replies;

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
    void concurrentPollersResendAnOverdueStepExactlyOnce() throws Exception {
        UUID sagaId = seedSaga(ORDER_SAGA, SagaStatus.STARTED, 1, "{}");
        UUID stepId = seedStepAt(sagaId, 1, StepStatus.SENT);
        makeOverdue(stepId, 1);
        OverdueStep snapshot = snapshot(sagaId, stepId, 1);

        List<Callable<Object>> tasks = new ArrayList<>();
        for (int i = 0; i < THREADS; i++) {
            tasks.add(() -> timeouts.handle(snapshot));
        }
        List<Object> results = race(tasks);

        assertThat(results).filteredOn(r -> r == Result.RESENT).hasSize(1);
        assertThat(results).filteredOn(r -> r != Result.RESENT).containsOnly(Result.LOST_RACE);
        assertThat(readStep(stepId).attemptCount()).isEqualTo(2);
        verify(publisher, times(1)).publish(anyString(), any(SagaCommand.class));
    }

    @RepeatedTest(10)
    void concurrentPollersEscalateAStepExactlyOnce() throws Exception {
        UUID sagaId = seedSaga(ORDER_SAGA, SagaStatus.STARTED, 1, "{}");
        UUID stepId = seedStepAt(sagaId, 1, StepStatus.SENT);
        makeOverdue(stepId, 5);
        OverdueStep snapshot = snapshot(sagaId, stepId, 5);

        List<Callable<Object>> tasks = new ArrayList<>();
        for (int i = 0; i < THREADS; i++) {
            tasks.add(() -> timeouts.handle(snapshot));
        }
        List<Object> results = race(tasks);

        assertThat(results).filteredOn(r -> r == Result.ESCALATED).hasSize(1);
        assertThat(results).filteredOn(r -> r != Result.ESCALATED).containsOnly(Result.LOST_RACE);
        assertThat(readStep(stepId).status()).isEqualTo(StepStatus.NEEDS_ATTENTION);
        assertThat(readSaga(sagaId).status()).isEqualTo(SagaStatus.NEEDS_ATTENTION);
        verify(alerts, times(1)).raise(eq(OpsAlertService.Type.STEP_NEEDS_ATTENTION), any(), any(), any());
        verifyNoInteractions(publisher);
    }

    @RepeatedTest(20)
    void resendNeverBlocksAReplyFromBeingApplied() throws Exception {
        UUID sagaId = seedSaga(ORDER_SAGA, SagaStatus.STARTED, 1, "{}");
        UUID stepId = seedStepAt(sagaId, 1, StepStatus.SENT);
        makeOverdue(stepId, 1);
        OverdueStep snapshot = snapshot(sagaId, stepId, 1);
        SagaReply reply = successReply(sagaId, stepId);

        List<Object> results = race(List.of(() -> timeouts.handle(snapshot), () -> replies.handle(reply)));

        assertThat(results.get(0)).isIn(Result.RESENT, Result.LOST_RACE);
        assertThat(results.get(1)).isEqualTo(ReplyHandler.Result.APPLIED);
        assertThat(readStep(stepId).status()).isEqualTo(StepStatus.SUCCESS);
        assertThat(readSaga(sagaId).currentStepSequence()).isEqualTo(2);
        verifyNoInteractions(alerts);
    }

    @RepeatedTest(20)
    void escalationRacingAReplyEndsInExactlyOneOutcomeAndNeverFailed() throws Exception {
        UUID sagaId = seedSaga(ORDER_SAGA, SagaStatus.STARTED, 1, "{}");
        UUID stepId = seedStepAt(sagaId, 1, StepStatus.SENT);
        makeOverdue(stepId, 5);
        OverdueStep snapshot = snapshot(sagaId, stepId, 5);
        SagaReply reply = successReply(sagaId, stepId);

        List<Object> results = race(List.of(() -> timeouts.handle(snapshot), () -> replies.handle(reply)));

        StepStatus finalStatus = readStep(stepId).status();
        assertThat(finalStatus).isIn(StepStatus.SUCCESS, StepStatus.NEEDS_ATTENTION);
        if (finalStatus == StepStatus.SUCCESS) {
            assertThat(results).containsExactly(Result.LOST_RACE, ReplyHandler.Result.APPLIED);
            assertThat(readSaga(sagaId).status()).isEqualTo(SagaStatus.STARTED);
            assertThat(readSaga(sagaId).currentStepSequence()).isEqualTo(2);
            verifyNoInteractions(alerts);
        } else {
            assertThat(results).containsExactly(Result.ESCALATED, ReplyHandler.Result.CONTRADICTION);
            assertThat(readSaga(sagaId).status()).isEqualTo(SagaStatus.NEEDS_ATTENTION);
            assertThat(countSteps(sagaId)).isEqualTo(1);
            verify(alerts).raise(eq(OpsAlertService.Type.STEP_NEEDS_ATTENTION), eq(sagaId), eq(stepId), any());
            verify(alerts).raise(eq(OpsAlertService.Type.REPLY_CONTRADICTS_LOG), eq(sagaId), eq(stepId), any());
            verify(publisher, never()).publish(anyString(), any(SagaCommand.class));
        }
    }

    private OverdueStep snapshot(UUID sagaId, UUID stepId, int attemptCount) {
        StepRow row = readStep(stepId);
        return new OverdueStep(stepId, sagaId, 1, row.status(), attemptCount, row.commandId(),
                row.compensationCommandId());
    }

    private SagaReply successReply(UUID sagaId, UUID stepId) {
        return new SagaReply(sagaId, stepId, readStep(stepId).commandId(), StepAction.FORWARD, ReplyOutcome.SUCCESS, null);
    }

    private List<Object> race(List<Callable<Object>> tasks) throws Exception {
        CountDownLatch gate = new CountDownLatch(1);
        List<Future<Object>> futures = new ArrayList<>();
        for (Callable<Object> task : tasks) {
            futures.add(pool.submit(() -> {
                if (!gate.await(30, TimeUnit.SECONDS)) {
                    throw new AssertionError("gate timed out");
                }
                return task.call();
            }));
        }
        gate.countDown();
        List<Object> results = new ArrayList<>();
        for (Future<Object> future : futures) {
            results.add(future.get(30, TimeUnit.SECONDS));
        }
        return results;
    }
}
