ALTER TABLE saga_step_log DROP CONSTRAINT chk_saga_step_log_status;
ALTER TABLE saga_step_log
    ADD CONSTRAINT chk_saga_step_log_status
        CHECK (status IN (
                          'PENDING',
                          'SENT',
                          'SUCCESS',
                          'FAILED',
                          'COMPENSATING',
                          'COMPENSATED',
                          'COMPENSATION_FAILED',
                          'NEEDS_ATTENTION'
            ));

ALTER TABLE saga_instance DROP CONSTRAINT chk_saga_instance_status;
ALTER TABLE saga_instance
    ADD CONSTRAINT chk_saga_instance_status
        CHECK (status IN (
                          'STARTED',
                          'COMPENSATING',
                          'COMPLETED',
                          'FAILED',
                          'COMPENSATION_FAILED',
                          'NEEDS_ATTENTION'
            ));

DROP INDEX idx_saga_step_log_sent_timeout;
CREATE INDEX idx_saga_step_log_in_flight_timeout
    ON saga_step_log (timeout_at)
    WHERE status IN ('SENT', 'COMPENSATING');

DROP INDEX idx_saga_step_log_pending_stale;
