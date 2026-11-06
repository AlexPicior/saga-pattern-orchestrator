package com.service.orchestrator;

import com.service.orchestrator.entity.SagaStatus;
import com.service.orchestrator.entity.StepStatus;
import org.junit.jupiter.api.BeforeEach;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.jdbc.core.simple.JdbcClient;

import java.time.OffsetDateTime;
import java.util.UUID;

@SpringBootTest(properties = {
        "spring.kafka.admin.auto-create=false",
        "saga.reply-listener.auto-startup=false",
        "saga.scheduling.enabled=false"})
@Import(TestcontainersConfiguration.class)
public abstract class AbstractPostgresTest {

    @Autowired
    protected JdbcClient jdbc;

    public record StepRow(StepStatus status, UUID commandId, UUID compensationCommandId, int attemptCount,
                          OffsetDateTime sentAt, OffsetDateTime timeoutAt, OffsetDateTime updatedAt) {
    }

    public record SagaRow(SagaStatus status, int currentStepSequence, OffsetDateTime updatedAt) {
    }

    @BeforeEach
    void cleanTables() {
        jdbc.sql("DELETE FROM saga_step_log").update();
        jdbc.sql("DELETE FROM saga_instance").update();
    }

    protected UUID seedStep(StepStatus status, int attemptCount) {
        UUID sagaId = UUID.randomUUID();
        jdbc.sql("""
                        INSERT INTO saga_instance (saga_id, saga_type, status, current_step_sequence)
                        VALUES (:sagaId, 'TEST', 'STARTED', 1)
                        """)
                .param("sagaId", sagaId)
                .update();

        UUID stepId = UUID.randomUUID();
        jdbc.sql("""
                        INSERT INTO saga_step_log
                            (step_id, saga_id, sequence, step_name, status, command_id, compensation_command_id,
                             attempt_count, updated_at)
                        VALUES
                            (:stepId, :sagaId, 1, 'TEST_STEP', :status, :commandId,
                             CASE WHEN :status IN ('COMPENSATING', 'COMPENSATED', 'COMPENSATION_FAILED')
                                  THEN gen_random_uuid() END,
                             :attemptCount, now() - interval '1 hour')
                        """)
                .param("stepId", stepId)
                .param("sagaId", sagaId)
                .param("status", status.name())
                .param("commandId", UUID.randomUUID())
                .param("attemptCount", attemptCount)
                .update();
        return stepId;
    }

    protected UUID seedSaga(String sagaType, SagaStatus status, int currentStepSequence, String payloadJson) {
        UUID sagaId = UUID.randomUUID();
        jdbc.sql("""
                        INSERT INTO saga_instance (saga_id, saga_type, status, current_step_sequence, payload, updated_at)
                        VALUES (:sagaId, :sagaType, :status, :currentStepSequence, CAST(:payload AS jsonb),
                                now() - interval '1 hour')
                        """)
                .param("sagaId", sagaId)
                .param("sagaType", sagaType)
                .param("status", status.name())
                .param("currentStepSequence", currentStepSequence)
                .param("payload", payloadJson)
                .update();
        return sagaId;
    }

    protected UUID seedStepAt(UUID sagaId, int sequence, StepStatus status) {
        UUID stepId = UUID.randomUUID();
        jdbc.sql("""
                        INSERT INTO saga_step_log
                            (step_id, saga_id, sequence, step_name, status, command_id, compensation_command_id,
                             attempt_count, updated_at)
                        VALUES
                            (:stepId, :sagaId, :sequence, 'TEST_STEP', :status, :commandId,
                             CASE WHEN :status IN ('COMPENSATING', 'COMPENSATED', 'COMPENSATION_FAILED')
                                  THEN gen_random_uuid() END,
                             1, now() - interval '1 hour')
                        """)
                .param("stepId", stepId)
                .param("sagaId", sagaId)
                .param("sequence", sequence)
                .param("status", status.name())
                .param("commandId", UUID.randomUUID())
                .update();
        return stepId;
    }

    protected void makeOverdue(UUID stepId, int attemptCount) {
        jdbc.sql("""
                        UPDATE saga_step_log
                           SET attempt_count = :attemptCount,
                               sent_at = now() - interval '2 minutes',
                               timeout_at = now() - interval '1 minute'
                         WHERE step_id = :stepId
                        """)
                .param("stepId", stepId)
                .param("attemptCount", attemptCount)
                .update();
    }

    protected void makeNotDueYet(UUID stepId) {
        jdbc.sql("""
                        UPDATE saga_step_log
                           SET sent_at = now(),
                               timeout_at = now() + interval '1 hour'
                         WHERE step_id = :stepId
                        """)
                .param("stepId", stepId)
                .update();
    }

    protected SagaRow readSaga(UUID sagaId) {
        return jdbc.sql("""
                        SELECT status, current_step_sequence, updated_at
                          FROM saga_instance
                         WHERE saga_id = :sagaId
                        """)
                .param("sagaId", sagaId)
                .query((rs, n) -> new SagaRow(
                        SagaStatus.valueOf(rs.getString("status")),
                        rs.getInt("current_step_sequence"),
                        rs.getObject("updated_at", OffsetDateTime.class)))
                .single();
    }

    protected int countSteps(UUID sagaId) {
        return jdbc.sql("SELECT count(*) FROM saga_step_log WHERE saga_id = :sagaId")
                .param("sagaId", sagaId)
                .query(Integer.class)
                .single();
    }

    protected UUID stepIdAt(UUID sagaId, int sequence) {
        return jdbc.sql("SELECT step_id FROM saga_step_log WHERE saga_id = :sagaId AND sequence = :sequence")
                .param("sagaId", sagaId)
                .param("sequence", sequence)
                .query(UUID.class)
                .single();
    }

    protected StepRow readStep(UUID stepId) {
        return jdbc.sql("""
                        SELECT status, command_id, compensation_command_id, attempt_count, sent_at, timeout_at, updated_at
                          FROM saga_step_log
                         WHERE step_id = :stepId
                        """)
                .param("stepId", stepId)
                .query((rs, n) -> new StepRow(
                        StepStatus.valueOf(rs.getString("status")),
                        rs.getObject("command_id", UUID.class),
                        rs.getObject("compensation_command_id", UUID.class),
                        rs.getInt("attempt_count"),
                        rs.getObject("sent_at", OffsetDateTime.class),
                        rs.getObject("timeout_at", OffsetDateTime.class),
                        rs.getObject("updated_at", OffsetDateTime.class)))
                .single();
    }
}
