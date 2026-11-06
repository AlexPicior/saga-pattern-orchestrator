package com.service.orchestrator.entity;

public enum SagaStatus {
    STARTED,
    COMPENSATING,
    COMPLETED,
    FAILED,
    COMPENSATION_FAILED,
    NEEDS_ATTENTION
}
