package com.service.orchestrator.service;

import com.service.orchestrator.definition.SagaDefinition;
import com.service.orchestrator.definition.SagaDefinitionRegistry;
import com.service.orchestrator.definition.StepDefinition;
import com.service.orchestrator.dto.SagaCommand;
import com.service.orchestrator.dto.StepAction;
import com.service.orchestrator.entity.SagaInstance;
import com.service.orchestrator.entity.SagaStepLog;
import com.service.orchestrator.event.CommandDispatchedEvent;
import com.service.orchestrator.repository.SagaInstanceRepository;
import com.service.orchestrator.repository.SagaStepLogRepository;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.UUID;

import static com.service.orchestrator.entity.SagaStatus.COMPENSATING;
import static com.service.orchestrator.entity.SagaStatus.COMPLETED;
import static com.service.orchestrator.entity.SagaStatus.STARTED;
import static com.service.orchestrator.entity.StepStatus.PENDING;
import static com.service.orchestrator.entity.StepStatus.SENT;

@Service
public class SagaOrchestrator {

    public enum Outcome {
        STEP_DISPATCHED,
        COMPLETED,
        COMPENSATION_DISPATCHED,
        ROLLED_BACK,
        COMPENSATION_FAILED,
        ESCALATED,
        WAITING,
        NOTHING_TO_DO,
        LOST_RACE
    }

    private final SagaInstanceRepository sagas;
    private final SagaStepLogRepository steps;
    private final SagaTransitionService sagaTransitions;
    private final StepTransitionService stepTransitions;
    private final ApplicationEventPublisher events;
    private final SagaDefinitionRegistry definitions;
    private final CompensationWalker walker;

    public SagaOrchestrator(SagaInstanceRepository sagas,
                            SagaStepLogRepository steps,
                            SagaTransitionService sagaTransitions,
                            StepTransitionService stepTransitions,
                            ApplicationEventPublisher events,
                            SagaDefinitionRegistry definitions,
                            CompensationWalker walker) {
        this.sagas = sagas;
        this.steps = steps;
        this.sagaTransitions = sagaTransitions;
        this.stepTransitions = stepTransitions;
        this.events = events;
        this.definitions = definitions;
        this.walker = walker;
    }

    @Transactional
    public Outcome advanceSaga(UUID sagaId) {
        SagaInstance saga = sagas.findById(sagaId)
                .orElseThrow(() -> new IllegalArgumentException("Unknown saga " + sagaId));
        if (saga.getStatus() == COMPENSATING) {
            return walker.walk(saga, definitions.get(saga.getSagaType()));
        }
        if (saga.getStatus() != STARTED) {
            return Outcome.NOTHING_TO_DO;
        }
        SagaDefinition definition = definitions.get(saga.getSagaType());
        int current = saga.getCurrentStepSequence();
        if (current == 0) {
            return startStep(saga, definition, 1);
        }
        SagaStepLog step = steps.findBySagaIdAndSequence(sagaId, current)
                .orElseThrow(() -> new IllegalStateException(
                        "Saga " + sagaId + " points at step " + current + " but that step does not exist"));
        return switch (step.getStatus()) {
            case PENDING, SENT -> Outcome.WAITING;
            case SUCCESS -> definition.isLast(current)
                    ? finish(sagaId)
                    : startStep(saga, definition, current + 1);
            case FAILED -> startCompensation(saga, definition);
            case COMPENSATING, COMPENSATED, COMPENSATION_FAILED, NEEDS_ATTENTION -> throw new IllegalStateException(
                    "Saga " + sagaId + " is STARTED but step " + current + " is " + step.getStatus());
        };
    }

    private Outcome startStep(SagaInstance saga, SagaDefinition definition, int sequence) {
        UUID sagaId = saga.getSagaId();
        if (!sagaTransitions.advanceStep(sagaId, sequence - 1)) {
            return Outcome.LOST_RACE;
        }
        StepDefinition stepDefinition = definition.step(sequence);
        SagaStepLog step = steps.saveAndFlush(
                SagaStepLog.pending(sagaId, sequence, stepDefinition.name(), Instant.now()));
        if (!stepTransitions.dispatch(step.getStepId(), PENDING, SENT, stepDefinition.timeout())) {
            throw new IllegalStateException("Step " + step.getStepId() + " was created in this transaction but could not be dispatched");
        }
        SagaCommand command = new SagaCommand(sagaId, step.getStepId(), step.getCommandId(),
                StepAction.FORWARD, 1, saga.getPayload());
        events.publishEvent(new CommandDispatchedEvent(stepDefinition.topic(), command));
        return Outcome.STEP_DISPATCHED;
    }

    private Outcome finish(UUID sagaId) {
        return sagaTransitions.transition(sagaId, STARTED, COMPLETED) ? Outcome.COMPLETED : Outcome.LOST_RACE;
    }

    private Outcome startCompensation(SagaInstance saga, SagaDefinition definition) {
        if (!sagaTransitions.transition(saga.getSagaId(), STARTED, COMPENSATING)) {
            return Outcome.LOST_RACE;
        }
        return walker.walk(saga, definition);
    }

}
