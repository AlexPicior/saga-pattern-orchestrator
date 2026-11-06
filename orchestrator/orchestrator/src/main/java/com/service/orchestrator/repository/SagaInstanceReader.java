package com.service.orchestrator.repository;

import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;

import java.time.Duration;
import java.util.List;
import java.util.UUID;

@Repository
public class SagaInstanceReader {

    private final JdbcClient jdbc;

    public SagaInstanceReader(JdbcClient jdbc) {
        this.jdbc = jdbc;
    }

    public List<UUID> findStaleWithNothingInFlight(Duration staleAfter, int limit) {
        return jdbc.sql("""
                        SELECT s.saga_id
                          FROM saga_instance s
                         WHERE s.status IN ('STARTED', 'COMPENSATING')
                           AND s.updated_at < now() - make_interval(secs => :staleSecs)
                           AND NOT EXISTS (SELECT 1
                                             FROM saga_step_log l
                                            WHERE l.saga_id = s.saga_id
                                              AND l.status IN ('PENDING', 'SENT', 'COMPENSATING'))
                         ORDER BY s.updated_at
                         LIMIT :limit
                        """)
                .param("staleSecs", staleAfter.toMillis() / 1000.0)
                .param("limit", limit)
                .query(UUID.class)
                .list();
    }
}
