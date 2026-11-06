package com.service.orchestrator.failure;

public enum FailureKind {
    BUSINESS,
    TRANSIENT,
    PERMANENT;

    public boolean isRetryable() {
        return this == TRANSIENT;
    }
}
