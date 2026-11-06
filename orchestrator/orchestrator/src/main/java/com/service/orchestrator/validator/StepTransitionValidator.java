package com.service.orchestrator.validator;

import com.service.orchestrator.entity.StepStatus;

import java.util.EnumMap;
import java.util.EnumSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import static com.service.orchestrator.entity.StepStatus.*;


public final class StepTransitionValidator {

    private static final Map<StepStatus, Set<StepStatus>> ALLOWED = buildTable();

    private StepTransitionValidator() {
    }

    public static boolean isValidTransition(StepStatus from, StepStatus to) {
        Objects.requireNonNull(from, "from");
        Objects.requireNonNull(to, "to");
        return ALLOWED.get(from).contains(to);
    }

    public static void requireValid(StepStatus from, StepStatus to) {
        if (!isValidTransition(from, to)) {
            throw new IllegalArgumentException("Invalid step transition: " + from + " -> " + to);
        }
    }

    public static boolean isTerminal(StepStatus status) {
        Objects.requireNonNull(status, "status");
        return ALLOWED.get(status).isEmpty();
    }

    private static Map<StepStatus, Set<StepStatus>> buildTable() {
        Map<StepStatus, Set<StepStatus>> t = new EnumMap<>(StepStatus.class);
        t.put(PENDING, EnumSet.of(SENT));
        t.put(SENT, EnumSet.of(SENT, SUCCESS, FAILED, NEEDS_ATTENTION));
        t.put(SUCCESS, EnumSet.of(COMPENSATING));
        t.put(FAILED, EnumSet.noneOf(StepStatus.class));
        t.put(COMPENSATING, EnumSet.of(COMPENSATING, COMPENSATED, COMPENSATION_FAILED, NEEDS_ATTENTION));
        t.put(COMPENSATED, EnumSet.noneOf(StepStatus.class));
        t.put(COMPENSATION_FAILED, EnumSet.noneOf(StepStatus.class));
        t.put(NEEDS_ATTENTION, EnumSet.noneOf(StepStatus.class));
        if (t.size() != StepStatus.values().length) {
            throw new IllegalStateException("Transition table must cover every StepStatus");
        }
        return t;
    }
}