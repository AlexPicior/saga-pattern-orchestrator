package com.service.orchestrator.failure;

import org.springframework.kafka.listener.ExceptionClassifier;
import org.springframework.stereotype.Component;

import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;

/**
 * Only a FAILED reply is a business failure. A timeout or an exception proves
 * nothing about what happened downstream, so it is never classified as one.
 */
@Component
public class FailureClassifier {

    private static final List<Class<? extends Throwable>> PERMANENT = Stream.concat(
                    ExceptionClassifier.defaultFatalExceptionsList().stream(),
                    Stream.<Class<? extends Throwable>>of(IllegalArgumentException.class))
            .toList();

    public FailureKind classify(StepFailure failure) {
        return switch (failure) {
            case StepFailure.ReplyFailed ignored -> FailureKind.BUSINESS;
            case StepFailure.TimedOut ignored -> FailureKind.TRANSIENT;
            case StepFailure.Thrown thrown -> isPermanent(thrown.cause()) ? FailureKind.PERMANENT : FailureKind.TRANSIENT;
        };
    }

    public List<Class<? extends Throwable>> permanentExceptions() {
        return PERMANENT;
    }

    private static boolean isPermanent(Throwable throwable) {
        Set<Throwable> seen = Collections.newSetFromMap(new IdentityHashMap<>());
        for (Throwable t = throwable; t != null && seen.add(t); t = t.getCause()) {
            for (Class<? extends Throwable> type : PERMANENT) {
                if (type.isInstance(t)) {
                    return true;
                }
            }
        }
        return false;
    }
}
