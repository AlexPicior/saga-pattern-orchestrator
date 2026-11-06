package com.service.orchestrator.service;

import com.service.orchestrator.entity.SagaStatus;
import com.service.orchestrator.validator.SagaTransitionValidator;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.util.Assert;

import java.util.UUID;

@Service
public class SagaTransitionService {

    private final JdbcClient jdbc;

    public SagaTransitionService(JdbcClient jdbc) {
        this.jdbc = jdbc;
    }

    @Transactional
    public boolean transition(UUID sagaId, SagaStatus from, SagaStatus to) {
        SagaTransitionValidator.requireValid(from, to);
        int rows = jdbc.sql("""
                        UPDATE saga_instance
                           SET status = :to,
                               updated_at = now()
                         WHERE saga_id = :sagaId
                           AND status = :from
                        """)
                .param("to", to.name())
                .param("sagaId", sagaId)
                .param("from", from.name())
                .update();
        return rows == 1;
    }

    @Transactional
    public boolean advanceStep(UUID sagaId, int fromSequence) {
        Assert.isTrue(fromSequence >= 0, "fromSequence must be >= 0");
        int rows = jdbc.sql("""
                        UPDATE saga_instance
                           SET current_step_sequence = :next,
                               updated_at = now()
                         WHERE saga_id = :sagaId
                           AND status = 'STARTED'
                           AND current_step_sequence = :from
                        """)
                .param("next", fromSequence + 1)
                .param("sagaId", sagaId)
                .param("from", fromSequence)
                .update();
        return rows == 1;
    }
}
