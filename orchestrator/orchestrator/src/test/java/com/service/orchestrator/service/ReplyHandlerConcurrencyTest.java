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
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

class ReplyHandlerConcurrencyTest extends AbstractPostgresTest {

    private static final int THREADS = 8;

    @MockitoBean
    private SagaCommandPublisher publisher;

    @MockitoBean
    private OpsAlertService alerts;

    @Autowired
    private ReplyHandler handler;

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
    void redeliveredSuccessReplyIsAppliedExactlyOnce() throws Exception {
        UUID sagaId = seedSaga(ORDER_SAGA, SagaStatus.STARTED, 1, "{}");
        UUID stepId = seedStepAt(sagaId, 1, StepStatus.SENT);
        SagaReply reply = reply(sagaId, stepId, ReplyOutcome.SUCCESS);

        List<Callable<Result>> tasks = new ArrayList<>();
        for (int i = 0; i < THREADS; i++) {
            tasks.add(() -> handler.handle(reply));
        }
        List<Result> results = race(tasks);

        assertThat(results).filteredOn(r -> r == Result.APPLIED).hasSize(1);
        assertThat(results).filteredOn(r -> r != Result.APPLIED).containsOnly(Result.DUPLICATE);
        assertThat(countSteps(sagaId)).isEqualTo(2);
        assertThat(readSaga(sagaId).currentStepSequence()).isEqualTo(2);
        verify(publisher, times(1)).publish(anyString(), any(SagaCommand.class));
    }

    @RepeatedTest(10)
    void conflictingRepliesLeaveExactlyOneOutcomeAndAlertOnTheRest() throws Exception {
        UUID sagaId = seedSaga(ORDER_SAGA, SagaStatus.STARTED, 1, "{}");
        UUID stepId = seedStepAt(sagaId, 1, StepStatus.SENT);

        List<Callable<Result>> tasks = new ArrayList<>();
        for (int i = 0; i < THREADS; i++) {
            ReplyOutcome outcome = i % 2 == 0 ? ReplyOutcome.SUCCESS : ReplyOutcome.FAILED;
            tasks.add(() -> handler.handle(reply(sagaId, stepId, outcome)));
        }
        List<Result> results = race(tasks);

        assertThat(results).filteredOn(r -> r == Result.APPLIED).hasSize(1);
        StepStatus finalStatus = readStep(stepId).status();
        assertThat(finalStatus).isIn(StepStatus.SUCCESS, StepStatus.FAILED);
        assertThat(readSaga(sagaId).status())
                .isEqualTo(finalStatus == StepStatus.SUCCESS ? SagaStatus.STARTED : SagaStatus.FAILED);
        long contradictions = results.stream().filter(r -> r == Result.CONTRADICTION).count();
        verify(alerts, times((int) contradictions)).raise(any(), any(), any(), any());
        assertThat(contradictions).isEqualTo(THREADS / 2);
    }

    private SagaReply reply(UUID sagaId, UUID stepId, ReplyOutcome outcome) {
        return new SagaReply(sagaId, stepId, readStep(stepId).commandId(), StepAction.FORWARD, outcome, null);
    }

    private List<Result> race(List<Callable<Result>> tasks) throws Exception {
        CountDownLatch gate = new CountDownLatch(1);
        List<Future<Result>> futures = new ArrayList<>();
        for (Callable<Result> task : tasks) {
            futures.add(pool.submit(() -> {
                if (!gate.await(30, TimeUnit.SECONDS)) {
                    throw new AssertionError("gate timed out");
                }
                return task.call();
            }));
        }
        gate.countDown();
        List<Result> results = new ArrayList<>();
        for (Future<Result> future : futures) {
            results.add(future.get(30, TimeUnit.SECONDS));
        }
        return results;
    }
}
