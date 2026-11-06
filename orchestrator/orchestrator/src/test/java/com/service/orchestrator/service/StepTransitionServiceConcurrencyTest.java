package com.service.orchestrator.service;

import com.service.orchestrator.AbstractPostgresTest;
import com.service.orchestrator.entity.StepStatus;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static com.service.orchestrator.entity.StepStatus.*;
import static org.assertj.core.api.Assertions.assertThat;

class StepTransitionServiceConcurrencyTest extends AbstractPostgresTest {

    private static final int THREADS = 8;
    private static final Duration TIMEOUT = Duration.ofSeconds(30);

    @Autowired
    private StepTransitionService service;

    @Autowired
    private PlatformTransactionManager transactionManager;

    private TransactionTemplate tx;
    private ExecutorService pool;

    @BeforeEach
    void setUp() {
        tx = new TransactionTemplate(transactionManager);
        pool = Executors.newFixedThreadPool(THREADS);
    }

    @AfterEach
    void tearDown() {
        pool.shutdownNow();
    }

    private record Attempt(StepStatus target, boolean won) {
    }

    @RepeatedTest(20)
    void racingResponsesOnOneSentStepHaveExactlyOneWinner() throws Exception {
        UUID stepId = seedStep(SENT, 1);

        List<Callable<Attempt>> racers = new ArrayList<>();
        for (int i = 0; i < THREADS; i++) {
            StepStatus target = i % 2 == 0 ? SUCCESS : FAILED;
            racers.add(() -> new Attempt(target, inTx(() -> service.transition(stepId, SENT, target))));
        }
        List<Attempt> attempts = race(racers);

        List<Attempt> winners = attempts.stream().filter(Attempt::won).toList();
        assertThat(winners).hasSize(1);
        assertThat(readStep(stepId).status()).isEqualTo(winners.getFirst().target());
        assertThat(readStep(stepId).attemptCount()).isEqualTo(1);
    }

    @RepeatedTest(20)
    void racingPollersRedispatchingTheSameObservationHaveExactlyOneWinner() throws Exception {
        UUID stepId = seedStep(SENT, 1);

        List<Callable<Boolean>> pollers = new ArrayList<>();
        for (int i = 0; i < THREADS; i++) {
            pollers.add(() -> inTx(() -> service.redispatch(stepId, SENT, 1, TIMEOUT)));
        }
        List<Boolean> results = race(pollers);

        assertThat(results).filteredOn(won -> won).hasSize(1);
        assertThat(readStep(stepId).attemptCount()).isEqualTo(2);

        assertThat(service.redispatch(stepId, SENT, 1, TIMEOUT)).isFalse();
        assertThat(service.redispatch(stepId, SENT, 2, TIMEOUT)).isTrue();
        assertThat(readStep(stepId).attemptCount()).isEqualTo(3);
    }

    @Test
    void secondWriterBlocksOnRowLockThenLosesOnceFirstCommits() throws Exception {
        UUID stepId = seedStep(SENT, 1);
        CountDownLatch aHasUpdated = new CountDownLatch(1);
        CountDownLatch releaseA = new CountDownLatch(1);

        Future<Boolean> a = pool.submit(() -> inTx(() -> {
            boolean won = service.transition(stepId, SENT, SUCCESS);
            aHasUpdated.countDown();
            await(releaseA);
            return won;
        }));
        assertThat(aHasUpdated.await(10, TimeUnit.SECONDS)).isTrue();

        Future<Boolean> b = pool.submit(() -> inTx(() -> service.transition(stepId, SENT, FAILED)));

        awaitBackendWaitingOnRowLock();
        assertThat(b.isDone()).isFalse();

        releaseA.countDown();
        assertThat(a.get(10, TimeUnit.SECONDS)).isTrue();
        assertThat(b.get(10, TimeUnit.SECONDS)).isFalse();
        assertThat(readStep(stepId).status()).isEqualTo(SUCCESS);
    }

    @Test
    void staleTimeoutCheckerLosesToListenerThatAdvancedTheStep() {
        UUID stepId = seedStep(SENT, 1);

        StepRow observed = readStep(stepId);
        assertThat(observed.status()).isEqualTo(SENT);

        assertThat(service.transition(stepId, SENT, SUCCESS)).isTrue();

        assertThat(service.transition(stepId, observed.status(), FAILED)).isFalse();
        assertThat(service.redispatch(stepId, observed.status(), observed.attemptCount(), TIMEOUT)).isFalse();

        StepRow after = readStep(stepId);
        assertThat(after.status()).isEqualTo(SUCCESS);
        assertThat(after.attemptCount()).isEqualTo(1);
    }

    private boolean inTx(Callable<Boolean> work) {
        Boolean result = tx.execute(status -> {
            try {
                return work.call();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
        return Boolean.TRUE.equals(result);
    }

    private <T> List<T> race(List<Callable<T>> tasks) throws Exception {
        CountDownLatch gate = new CountDownLatch(1);
        List<Future<T>> futures = new ArrayList<>();
        for (Callable<T> task : tasks) {
            futures.add(pool.submit(() -> {
                await(gate);
                return task.call();
            }));
        }
        gate.countDown();
        List<T> results = new ArrayList<>();
        for (Future<T> f : futures) {
            results.add(f.get(30, TimeUnit.SECONDS));
        }
        return results;
    }

    private void awaitBackendWaitingOnRowLock() throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        while (System.nanoTime() < deadline) {
            int waiting = jdbc.sql("""
                            SELECT count(*)
                              FROM pg_stat_activity
                             WHERE datname = current_database()
                               AND pid <> pg_backend_pid()
                               AND wait_event_type = 'Lock'
                               AND query ILIKE '%UPDATE saga_step_log%'
                            """)
                    .query(Integer.class)
                    .single();
            if (waiting > 0) {
                return;
            }
            Thread.sleep(20);
        }
        throw new AssertionError("second UPDATE never blocked on the row lock");
    }

    private static void await(CountDownLatch latch) throws InterruptedException {
        if (!latch.await(30, TimeUnit.SECONDS)) {
            throw new AssertionError("latch timed out");
        }
    }
}
