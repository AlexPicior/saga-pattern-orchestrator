package com.service.orchestrator.validator;

import com.service.orchestrator.entity.SagaStatus;

import java.util.EnumMap;
import java.util.EnumSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import static com.service.orchestrator.entity.SagaStatus.*;

public final class SagaTransitionValidator {

    private static final Map<SagaStatus, Set<SagaStatus>> ALLOWED = buildTable();

    private SagaTransitionValidator() {
    }

    public static boolean isValidTransition(SagaStatus from, SagaStatus to) {
        Objects.requireNonNull(from, "from");
        Objects.requireNonNull(to, "to");
        return ALLOWED.get(from).contains(to);
    }

    public static void requireValid(SagaStatus from, SagaStatus to) {
        if (!isValidTransition(from, to)) {
            throw new IllegalArgumentException("Invalid saga transition: " + from + " -> " + to);
        }
    }

    public static boolean isTerminal(SagaStatus status) {
        Objects.requireNonNull(status, "status");
        return ALLOWED.get(status).isEmpty();
    }

    private static Map<SagaStatus, Set<SagaStatus>> buildTable() {
        Map<SagaStatus, Set<SagaStatus>> t = new EnumMap<>(SagaStatus.class);
        t.put(STARTED, EnumSet.of(COMPLETED, COMPENSATING, NEEDS_ATTENTION));
        t.put(COMPENSATING, EnumSet.of(FAILED, COMPENSATION_FAILED, NEEDS_ATTENTION));
        t.put(COMPLETED, EnumSet.noneOf(SagaStatus.class));
        t.put(FAILED, EnumSet.noneOf(SagaStatus.class));
        t.put(COMPENSATION_FAILED, EnumSet.noneOf(SagaStatus.class));
        t.put(NEEDS_ATTENTION, EnumSet.noneOf(SagaStatus.class));
        if (t.size() != SagaStatus.values().length) {
            throw new IllegalStateException("Transition table must cover every SagaStatus");
        }
        return t;
    }
}
