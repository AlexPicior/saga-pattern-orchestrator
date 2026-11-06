package com.service.orchestrator.scheduler;

import com.service.orchestrator.repository.StepLogReader;
import com.service.orchestrator.repository.StepLogReader.OverdueStep;
import com.service.orchestrator.service.StepTimeoutHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.List;

@Component
public class OverdueStepPoller {

    private static final Logger log = LoggerFactory.getLogger(OverdueStepPoller.class);

    private final StepLogReader reader;
    private final StepTimeoutHandler handler;
    private final int batchSize;

    public OverdueStepPoller(StepLogReader reader,
                             StepTimeoutHandler handler,
                             @Value("${saga.poller.batch-size:100}") int batchSize) {
        this.reader = reader;
        this.handler = handler;
        this.batchSize = batchSize;
    }

    @Scheduled(fixedDelayString = "${saga.poller.interval:5s}")
    public void poll() {
        pollOnce();
    }

    public int pollOnce() {
        List<OverdueStep> overdue = reader.findOverdue(batchSize);
        for (OverdueStep step : overdue) {
            try {
                StepTimeoutHandler.Result result = handler.handle(step);
                log.info("Overdue {} step {} of saga {} (attempt {}): {}",
                        step.status(), step.stepId(), step.sagaId(), step.attemptCount(), result);
            } catch (RuntimeException e) {
                log.warn("Could not handle overdue step {} of saga {}", step.stepId(), step.sagaId(), e);
            }
        }
        return overdue.size();
    }
}
