package com.service.orchestrator.service;

import com.service.orchestrator.entity.StepStatus;
import com.service.orchestrator.validator.StepTransitionValidator;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Duration;
import java.util.Objects;
import java.util.UUID;

import static com.service.orchestrator.entity.StepStatus.COMPENSATING;
import static com.service.orchestrator.entity.StepStatus.NEEDS_ATTENTION;
import static com.service.orchestrator.entity.StepStatus.SENT;

@Service
public class StepTransitionService {

    private final JdbcClient jdbc;

    public StepTransitionService(JdbcClient jdbc) {
        this.jdbc = jdbc;
    }

    @Transactional
    public boolean transition(UUID stepId, StepStatus from, StepStatus to) {
        StepTransitionValidator.requireValid(from, to);
        if (isSendEdge(to)) {
            throw new IllegalArgumentException(
                    from + " -> " + to + " sends a command; use dispatch() or redispatch()");
        }
        if (to == NEEDS_ATTENTION) {
            throw new IllegalArgumentException(from + " -> " + to + " is an escalation; use escalate()");
        }
        int rows = jdbc.sql("""
                        UPDATE saga_step_log
                           SET status = :to,
                               updated_at = now()
                         WHERE step_id = :stepId
                           AND status = :from
                        """)
                .param("to", to.name())
                .param("stepId", stepId)
                .param("from", from.name())
                .update();
        return rows == 1;
    }

    @Transactional
    public boolean escalate(UUID stepId, StepStatus status, int observedAttemptCount) {
        StepTransitionValidator.requireValid(status, NEEDS_ATTENTION);
        int rows = jdbc.sql("""
                        UPDATE saga_step_log
                           SET status = 'NEEDS_ATTENTION',
                               updated_at = now()
                         WHERE step_id = :stepId
                           AND status = :status
                           AND attempt_count = :observed
                        """)
                .param("stepId", stepId)
                .param("status", status.name())
                .param("observed", observedAttemptCount)
                .update();
        return rows == 1;
    }

    @Transactional
    public boolean dispatch(UUID stepId, StepStatus from, StepStatus to, Duration timeout) {
        StepTransitionValidator.requireValid(from, to);
        if (from == to || !isSendEdge(to)) {
            throw new IllegalArgumentException(
                    from + " -> " + to + " is not a first send; use transition() or redispatch()");
        }
        int rows = jdbc.sql("""
                        UPDATE saga_step_log
                           SET status = :to,
                               attempt_count = 1,
                               compensation_command_id = CASE WHEN :to = 'COMPENSATING'
                                                              THEN gen_random_uuid()
                                                              ELSE compensation_command_id END,
                               sent_at = now(),
                               timeout_at = now() + make_interval(secs => :timeoutSecs),
                               updated_at = now()
                         WHERE step_id = :stepId
                           AND status = :from
                        """)
                .param("to", to.name())
                .param("timeoutSecs", toSeconds(timeout))
                .param("stepId", stepId)
                .param("from", from.name())
                .update();
        return rows == 1;
    }

    @Transactional
    public boolean redispatch(UUID stepId, StepStatus status, int observedAttemptCount, Duration timeout) {
        StepTransitionValidator.requireValid(status, status);
        int rows = jdbc.sql("""
                        UPDATE saga_step_log
                           SET attempt_count = attempt_count + 1,
                               sent_at = now(),
                               timeout_at = now() + make_interval(secs => :timeoutSecs),
                               updated_at = now()
                         WHERE step_id = :stepId
                           AND status = :status
                           AND attempt_count = :observed
                        """)
                .param("timeoutSecs", toSeconds(timeout))
                .param("stepId", stepId)
                .param("status", status.name())
                .param("observed", observedAttemptCount)
                .update();
        return rows == 1;
    }

    private static boolean isSendEdge(StepStatus to) {
        return to == SENT || to == COMPENSATING;
    }

    private static double toSeconds(Duration timeout) {
        Objects.requireNonNull(timeout, "timeout");
        if (timeout.isNegative() || timeout.isZero()) {
            throw new IllegalArgumentException("timeout must be positive: " + timeout);
        }
        return timeout.toMillis() / 1000.0;
    }
}
