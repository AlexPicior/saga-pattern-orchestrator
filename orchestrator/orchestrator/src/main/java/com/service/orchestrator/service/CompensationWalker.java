package com.service.orchestrator.service;

import com.service.orchestrator.definition.SagaDefinition;
import com.service.orchestrator.definition.StepDefinition;
import com.service.orchestrator.dto.SagaCommand;
import com.service.orchestrator.dto.StepAction;
import com.service.orchestrator.entity.SagaInstance;
import com.service.orchestrator.entity.SagaStatus;
import com.service.orchestrator.entity.StepStatus;
import com.service.orchestrator.event.CommandDispatchedEvent;
import com.service.orchestrator.repository.StepLogReader;
import com.service.orchestrator.repository.StepLogReader.SagaStep;
import com.service.orchestrator.service.SagaOrchestrator.Outcome;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;
import java.util.Optional;
import java.util.UUID;

@Service
public class CompensationWalker {

    private final StepLogReader reader;
    private final StepTransitionService stepTransitions;
    private final SagaTransitionService sagaTransitions;
    private final ApplicationEventPublisher events;
    private final OpsAlertService alerts;

    public CompensationWalker(StepLogReader reader,
                              StepTransitionService stepTransitions,
                              SagaTransitionService sagaTransitions,
                              ApplicationEventPublisher events,
                              OpsAlertService alerts) {
        this.reader = reader;
        this.stepTransitions = stepTransitions;
        this.sagaTransitions = sagaTransitions;
        this.events = events;
        this.alerts = alerts;
    }

    @Transactional(propagation = Propagation.MANDATORY)
    public Outcome walk(SagaInstance saga, SagaDefinition definition) {
        UUID sagaId = saga.getSagaId();
        List<SagaStep> steps = reader.findBySagaNewestFirst(sagaId);

        SagaStep failedCompensation = null;
        for (SagaStep step : steps) {
            switch (step.status()) {
                case COMPENSATING -> {
                    return Outcome.WAITING;
                }
                case COMPENSATION_FAILED -> failedCompensation = step;
                case PENDING, SENT, NEEDS_ATTENTION -> throw new IllegalStateException(
                        "Saga " + sagaId + " is COMPENSATING but step " + step.sequence() + " is " + step.status());
                case SUCCESS, FAILED, COMPENSATED -> {
                }
            }
        }
        if (failedCompensation != null) {
            return giveUp(sagaId, failedCompensation);
        }

        Optional<SagaStep> next = steps.stream().filter(s -> s.status() == StepStatus.SUCCESS).findFirst();
        if (next.isEmpty()) {
            return sagaTransitions.transition(sagaId, SagaStatus.COMPENSATING, SagaStatus.FAILED)
                    ? Outcome.ROLLED_BACK
                    : Outcome.LOST_RACE;
        }
        SagaStep step = next.get();
        StepDefinition stepDefinition = definition.step(step.sequence());
        if (!stepDefinition.isCompensatable()) {
            return refusePivot(sagaId, step, stepDefinition);
        }
        return compensate(saga, step, stepDefinition);
    }

    private Outcome compensate(SagaInstance saga, SagaStep step, StepDefinition stepDefinition) {
        if (!stepTransitions.dispatch(step.stepId(), StepStatus.SUCCESS, StepStatus.COMPENSATING,
                stepDefinition.timeout())) {
            return Outcome.LOST_RACE;
        }
        UUID compensationCommandId = reader.find(step.stepId()).orElseThrow().compensationCommandId();
        SagaCommand command = new SagaCommand(saga.getSagaId(), step.stepId(), compensationCommandId,
                StepAction.COMPENSATE, 1, saga.getPayload());
        events.publishEvent(new CommandDispatchedEvent(stepDefinition.topic(), command));
        return Outcome.COMPENSATION_DISPATCHED;
    }

    private Outcome giveUp(UUID sagaId, SagaStep step) {
        if (!sagaTransitions.transition(sagaId, SagaStatus.COMPENSATING, SagaStatus.COMPENSATION_FAILED)) {
            return Outcome.LOST_RACE;
        }
        alerts.raise(OpsAlertService.Type.COMPENSATION_FAILED, sagaId, step.stepId(),
                "compensation of step " + step.sequence() + " failed; the saga is only partly rolled back");
        return Outcome.COMPENSATION_FAILED;
    }

    private Outcome refusePivot(UUID sagaId, SagaStep step, StepDefinition stepDefinition) {
        if (!sagaTransitions.transition(sagaId, SagaStatus.COMPENSATING, SagaStatus.NEEDS_ATTENTION)) {
            return Outcome.LOST_RACE;
        }
        alerts.raise(OpsAlertService.Type.CANNOT_COMPENSATE_PIVOT, sagaId, step.stepId(),
                "step " + step.sequence() + " (" + stepDefinition.name() + ") is the " + stepDefinition.kind()
                        + " and already succeeded, so it cannot be compensated");
        return Outcome.ESCALATED;
    }
}
