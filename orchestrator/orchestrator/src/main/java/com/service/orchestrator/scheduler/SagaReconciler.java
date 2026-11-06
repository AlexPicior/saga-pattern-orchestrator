package com.service.orchestrator.scheduler;

import com.service.orchestrator.repository.SagaInstanceReader;
import com.service.orchestrator.service.SagaOrchestrator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.List;
import java.util.UUID;

@Component
public class SagaReconciler {

    private static final Logger log = LoggerFactory.getLogger(SagaReconciler.class);

    private final SagaInstanceReader reader;
    private final SagaOrchestrator orchestrator;
    private final Duration staleAfter;
    private final int batchSize;

    public SagaReconciler(SagaInstanceReader reader,
                          SagaOrchestrator orchestrator,
                          @Value("${saga.reconciler.stale-after:60s}") Duration staleAfter,
                          @Value("${saga.reconciler.batch-size:100}") int batchSize) {
        this.reader = reader;
        this.orchestrator = orchestrator;
        this.staleAfter = staleAfter;
        this.batchSize = batchSize;
    }

    @Scheduled(fixedDelayString = "${saga.reconciler.interval:30s}")
    public void reconcile() {
        sweepOnce();
    }

    public int sweepOnce() {
        List<UUID> stale = reader.findStaleWithNothingInFlight(staleAfter, batchSize);
        for (UUID sagaId : stale) {
            try {
                SagaOrchestrator.Outcome outcome = orchestrator.advanceSaga(sagaId);
                log.info("Reconciled stale saga {}: {}", sagaId, outcome);
            } catch (RuntimeException e) {
                log.warn("Could not reconcile saga {}", sagaId, e);
            }
        }
        return stale.size();
    }
}
