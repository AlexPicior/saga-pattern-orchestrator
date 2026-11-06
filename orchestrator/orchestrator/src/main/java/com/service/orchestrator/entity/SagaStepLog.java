package com.service.orchestrator.entity;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import lombok.Getter;

import java.time.Instant;
import java.util.UUID;

@Entity
@Table(name = "saga_step_log")
@Getter
public class SagaStepLog {

    @Id
    @GeneratedValue(strategy = GenerationType.UUID)
    @Column(name = "step_id", nullable = false, updatable = false)
    private UUID stepId;

    @Column(name = "saga_id", nullable = false, updatable = false)
    private UUID sagaId;

    /** 1-based. Unique per saga (enforced by uq_saga_step_log_saga_sequence). */
    @Column(name = "sequence", nullable = false, updatable = false)
    private int sequence;

    @Column(name = "step_name", nullable = false, updatable = false, length = 100)
    private String stepName;

    @Enumerated(EnumType.STRING)
    @Column(name = "status", nullable = false, updatable = false, length = 30)
    private StepStatus status;

    /** Generated once in pending() and reused for every resend of this step. */
    @Column(name = "command_id", nullable = false, updatable = false)
    private UUID commandId;

    @Column(name = "compensation_command_id", updatable = false)
    private UUID compensationCommandId;

    /** Incremented only when entering SENT / COMPENSATING (see StepTransitionValidator). */
    @Column(name = "attempt_count", nullable = false, updatable = false)
    private int attemptCount;

    @Column(name = "sent_at", updatable = false)
    private Instant sentAt;

    /**
     * Anything compared against this (is the step overdue?) must use the DB
     * clock, set in the same UPDATE that writes it, never a JVM clock: replicas
     * can disagree on the time, Postgres cannot.
     */
    @Column(name = "timeout_at", updatable = false)
    private Instant timeoutAt;

    @Column(name = "updated_at", nullable = false, updatable = false)
    private Instant updatedAt;

    protected SagaStepLog() {
        // for JPA
    }

    public static SagaStepLog pending(UUID sagaId, int sequence, String stepName, Instant now) {
        SagaStepLog s = new SagaStepLog();
        s.sagaId = sagaId;
        s.sequence = sequence;
        s.stepName = stepName;
        s.status = StepStatus.PENDING;
        s.commandId = UUID.randomUUID();
        s.attemptCount = 0;
        s.updatedAt = now;
        return s;
    }
}
