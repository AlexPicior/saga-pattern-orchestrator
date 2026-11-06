package com.service.orchestrator.entity;

public enum StepStatus {
    PENDING,
    SENT,
    SUCCESS,
    FAILED,
    COMPENSATING,
    COMPENSATED,
    COMPENSATION_FAILED,
    NEEDS_ATTENTION
}
