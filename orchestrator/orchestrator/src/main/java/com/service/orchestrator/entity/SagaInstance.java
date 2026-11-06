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
import org.hibernate.annotations.JdbcTypeCode;
import org.hibernate.type.SqlTypes;

import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;

@Entity
@Table(name = "saga_instance")
@Getter
public class SagaInstance {

    @Id
    @GeneratedValue(strategy = GenerationType.UUID)
    @Column(name = "saga_id", nullable = false, updatable = false)
    private UUID sagaId;

    @Column(name = "saga_type", nullable = false, updatable = false, length = 100)
    private String sagaType;

    @Enumerated(EnumType.STRING)
    @Column(name = "status", nullable = false, updatable = false, length = 30)
    private SagaStatus status;

    @Column(name = "current_step_sequence", nullable = false, updatable = false)
    private int currentStepSequence;

    @Column(name = "created_at", nullable = false, updatable = false)
    private Instant createdAt;

    @Column(name = "updated_at", nullable = false, updatable = false)
    private Instant updatedAt;

    @JdbcTypeCode(SqlTypes.JSON)
    @Column(name = "payload", nullable = false, updatable = false)
    private Map<String, Object> payload;

    protected SagaInstance() {
    }

    public static SagaInstance start(String sagaType, Map<String, Object> payload, Instant now) {
        SagaInstance s = new SagaInstance();
        s.sagaType = sagaType;
        s.status = SagaStatus.STARTED;
        s.currentStepSequence = 0;
        s.createdAt = now;
        s.updatedAt = now;
        s.payload = payload == null ? new LinkedHashMap<>() : new LinkedHashMap<>(payload);
        return s;
    }
}
