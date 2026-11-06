package com.service.orchestrator.service;

import com.service.orchestrator.definition.SagaDefinitionRegistry;
import com.service.orchestrator.definition.StepDefinition;
import com.service.orchestrator.dto.SagaCommand;
import com.service.orchestrator.dto.StepAction;
import com.service.orchestrator.entity.SagaInstance;
import com.service.orchestrator.entity.SagaStatus;
import com.service.orchestrator.event.CommandDispatchedEvent;
import com.service.orchestrator.repository.SagaInstanceRepository;
import com.service.orchestrator.repository.StepLogReader.OverdueStep;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.util.Assert;

import java.util.UUID;

import static com.service.orchestrator.entity.StepStatus.SENT;

@Service
public class StepTimeoutHandler {

    public enum Result {
        RESENT,
        ESCALATED,
        LOST_RACE
    }

    private final SagaInstanceRepository sagas;
    private final SagaDefinitionRegistry definitions;
    private final StepTransitionService stepTransitions;
    private final SagaTransitionService sagaTransitions;
    private final ApplicationEventPublisher events;
    private final OpsAlertService alerts;
    private final int maxAttempts;

    public StepTimeoutHandler(SagaInstanceRepository sagas,
                              SagaDefinitionRegistry definitions,
                              StepTransitionService stepTransitions,
                              SagaTransitionService sagaTransitions,
                              ApplicationEventPublisher events,
                              OpsAlertService alerts,
                              @Value("${saga.poller.max-attempts:5}") int maxAttempts) {
        Assert.isTrue(maxAttempts >= 1, "saga.poller.max-attempts must be >= 1");
        this.sagas = sagas;
        this.definitions = definitions;
        this.stepTransitions = stepTransitions;
        this.sagaTransitions = sagaTransitions;
        this.events = events;
        this.alerts = alerts;
        this.maxAttempts = maxAttempts;
    }

    @Transactional
    public Result handle(OverdueStep step) {
        SagaInstance saga = sagas.findById(step.sagaId())
                .orElseThrow(() -> new IllegalStateException("Step " + step.stepId() + " belongs to missing saga " + step.sagaId()));
        StepDefinition definition = definitions.get(saga.getSagaType()).step(step.sequence());

        if (step.attemptCount() < maxAttempts) {
            return resend(step, saga, definition);
        }
        return escalate(step);
    }

    private Result resend(OverdueStep step, SagaInstance saga, StepDefinition definition) {
        if (!stepTransitions.redispatch(step.stepId(), step.status(), step.attemptCount(), definition.timeout())) {
            return Result.LOST_RACE;
        }
        StepAction action = step.status() == SENT ? StepAction.FORWARD : StepAction.COMPENSATE;
        UUID commandId = action == StepAction.FORWARD ? step.commandId() : step.compensationCommandId();
        SagaCommand command = new SagaCommand(step.sagaId(), step.stepId(), commandId, action,
                step.attemptCount() + 1, saga.getPayload());
        events.publishEvent(new CommandDispatchedEvent(definition.topic(), command));
        return Result.RESENT;
    }

    private Result escalate(OverdueStep step) {
        if (!stepTransitions.escalate(step.stepId(), step.status(), step.attemptCount())) {
            return Result.LOST_RACE;
        }
        SagaStatus sagaFrom = step.status() == SENT ? SagaStatus.STARTED : SagaStatus.COMPENSATING;
        if (!sagaTransitions.transition(step.sagaId(), sagaFrom, SagaStatus.NEEDS_ATTENTION)) {
            throw new IllegalStateException("Step " + step.stepId() + " escalated but saga " + step.sagaId()
                    + " was not " + sagaFrom);
        }
        alerts.raise(OpsAlertService.Type.STEP_NEEDS_ATTENTION, step.sagaId(), step.stepId(),
                "no reply after " + step.attemptCount() + " attempts while " + step.status());
        return Result.ESCALATED;
    }
}
