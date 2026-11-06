package com.service.orchestrator.service;

import com.service.orchestrator.dto.ReplyOutcome;
import com.service.orchestrator.dto.SagaReply;
import com.service.orchestrator.dto.StepAction;
import com.service.orchestrator.entity.StepStatus;
import com.service.orchestrator.repository.StepLogReader;
import com.service.orchestrator.repository.StepLogReader.StepSnapshot;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.EnumSet;
import java.util.Set;
import java.util.UUID;

import static com.service.orchestrator.entity.StepStatus.*;

@Service
public class ReplyHandler {

    public enum Result {
        APPLIED,
        DUPLICATE,
        CONTRADICTION
    }

    private static final Set<StepStatus> AFTER_SUCCESS = EnumSet.of(COMPENSATING, COMPENSATED, COMPENSATION_FAILED);

    private final StepLogReader reader;
    private final StepTransitionService stepTransitions;
    private final SagaOrchestrator orchestrator;
    private final OpsAlertService alerts;

    public ReplyHandler(StepLogReader reader,
                        StepTransitionService stepTransitions,
                        SagaOrchestrator orchestrator,
                        OpsAlertService alerts) {
        this.reader = reader;
        this.stepTransitions = stepTransitions;
        this.orchestrator = orchestrator;
        this.alerts = alerts;
    }

    @Transactional
    public Result handle(SagaReply reply) {
        StepSnapshot step = reader.find(reply.stepId())
                .orElseThrow(() -> new IllegalArgumentException("Reply for unknown step " + reply.stepId()));
        requireBelongsTo(reply, step);

        StepStatus from = reply.action() == StepAction.FORWARD ? SENT : COMPENSATING;
        StepStatus to = targetStatus(reply);
        if (stepTransitions.transition(step.stepId(), from, to)) {
            orchestrator.advanceSaga(reply.sagaId());
            return Result.APPLIED;
        }

        StepStatus current = reader.currentStatus(step.stepId()).orElseThrow();
        if (current == to || (reply.action() == StepAction.FORWARD && to == SUCCESS && AFTER_SUCCESS.contains(current))) {
            return Result.DUPLICATE;
        }
        alerts.raise(OpsAlertService.Type.REPLY_CONTRADICTS_LOG, reply.sagaId(), reply.stepId(),
                reply.action() + " " + reply.outcome() + " reply (command " + reply.commandId()
                        + ") arrived but the step is " + current);
        return Result.CONTRADICTION;
    }

    private static void requireBelongsTo(SagaReply reply, StepSnapshot step) {
        if (!step.sagaId().equals(reply.sagaId())) {
            throw new IllegalArgumentException("Reply for step " + step.stepId() + " names saga " + reply.sagaId()
                    + " but the step belongs to saga " + step.sagaId());
        }
        UUID expected = reply.action() == StepAction.FORWARD ? step.commandId() : step.compensationCommandId();
        if (!reply.commandId().equals(expected)) {
            throw new IllegalArgumentException("Reply command " + reply.commandId() + " is not the "
                    + reply.action() + " command of step " + step.stepId());
        }
    }

    private static StepStatus targetStatus(SagaReply reply) {
        boolean success = reply.outcome() == ReplyOutcome.SUCCESS;
        return reply.action() == StepAction.FORWARD
                ? (success ? SUCCESS : FAILED)
                : (success ? COMPENSATED : COMPENSATION_FAILED);
    }
}
