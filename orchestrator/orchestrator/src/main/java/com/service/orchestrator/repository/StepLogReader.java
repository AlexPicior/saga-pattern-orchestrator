package com.service.orchestrator.repository;

import com.service.orchestrator.entity.StepStatus;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Optional;
import java.util.UUID;

/**
 * Reads steps without putting them in the JPA persistence context. A managed
 * SagaStepLog loaded early in a transaction keeps its old status after a
 * conditional UPDATE, and later JPA queries in the same transaction would
 * return that stale copy.
 */
@Repository
public class StepLogReader {

    public record StepSnapshot(UUID stepId, UUID sagaId, StepStatus status, UUID commandId,
                               UUID compensationCommandId) {
    }

    public record OverdueStep(UUID stepId, UUID sagaId, int sequence, StepStatus status, int attemptCount,
                              UUID commandId, UUID compensationCommandId) {
    }

    public record SagaStep(UUID stepId, int sequence, StepStatus status, UUID compensationCommandId) {
    }

    private final JdbcClient jdbc;

    public StepLogReader(JdbcClient jdbc) {
        this.jdbc = jdbc;
    }

    public Optional<StepSnapshot> find(UUID stepId) {
        return jdbc.sql("""
                        SELECT step_id, saga_id, status, command_id, compensation_command_id
                          FROM saga_step_log
                         WHERE step_id = :stepId
                        """)
                .param("stepId", stepId)
                .query((rs, n) -> new StepSnapshot(
                        rs.getObject("step_id", UUID.class),
                        rs.getObject("saga_id", UUID.class),
                        StepStatus.valueOf(rs.getString("status")),
                        rs.getObject("command_id", UUID.class),
                        rs.getObject("compensation_command_id", UUID.class)))
                .optional();
    }

    public List<SagaStep> findBySagaNewestFirst(UUID sagaId) {
        return jdbc.sql("""
                        SELECT step_id, sequence, status, compensation_command_id
                          FROM saga_step_log
                         WHERE saga_id = :sagaId
                         ORDER BY sequence DESC
                        """)
                .param("sagaId", sagaId)
                .query((rs, n) -> new SagaStep(
                        rs.getObject("step_id", UUID.class),
                        rs.getInt("sequence"),
                        StepStatus.valueOf(rs.getString("status")),
                        rs.getObject("compensation_command_id", UUID.class)))
                .list();
    }

    public List<OverdueStep> findOverdue(int limit) {
        return jdbc.sql("""
                        SELECT step_id, saga_id, sequence, status, attempt_count, command_id, compensation_command_id
                          FROM saga_step_log
                         WHERE status IN ('SENT', 'COMPENSATING')
                           AND timeout_at < now()
                         ORDER BY timeout_at
                         LIMIT :limit
                        """)
                .param("limit", limit)
                .query((rs, n) -> new OverdueStep(
                        rs.getObject("step_id", UUID.class),
                        rs.getObject("saga_id", UUID.class),
                        rs.getInt("sequence"),
                        StepStatus.valueOf(rs.getString("status")),
                        rs.getInt("attempt_count"),
                        rs.getObject("command_id", UUID.class),
                        rs.getObject("compensation_command_id", UUID.class)))
                .list();
    }

    public Optional<StepStatus> currentStatus(UUID stepId) {
        return jdbc.sql("SELECT status FROM saga_step_log WHERE step_id = :stepId")
                .param("stepId", stepId)
                .query((rs, n) -> StepStatus.valueOf(rs.getString("status")))
                .optional();
    }
}
