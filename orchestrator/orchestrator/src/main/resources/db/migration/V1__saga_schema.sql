CREATE TABLE saga_instance (
                               saga_id               UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                               saga_type             VARCHAR(100) NOT NULL,
                               status                VARCHAR(30) NOT NULL,
                               current_step_sequence INTEGER NOT NULL DEFAULT 0,
                               created_at            TIMESTAMPTZ NOT NULL DEFAULT now(),
                               updated_at            TIMESTAMPTZ NOT NULL DEFAULT now(),

                               CONSTRAINT chk_saga_instance_status
                                   CHECK (status IN (
                                                     'STARTED',
                                                     'COMPENSATING',
                                                     'COMPLETED',
                                                     'FAILED',
                                                     'COMPENSATION_FAILED'
                                       ))
);

CREATE TABLE saga_step_log (
                               step_id       UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                               saga_id       UUID NOT NULL REFERENCES saga_instance (saga_id),
                               sequence      INTEGER NOT NULL,
                               step_name     VARCHAR(100) NOT NULL,
                               status        VARCHAR(30) NOT NULL,
                               command_id    UUID NOT NULL,
                               attempt_count INTEGER NOT NULL DEFAULT 0,
                               sent_at       TIMESTAMPTZ,
                               timeout_at    TIMESTAMPTZ,
                               updated_at    TIMESTAMPTZ NOT NULL DEFAULT now(),

                               CONSTRAINT chk_saga_step_log_status
                                   CHECK (status IN (
                                                     'PENDING',
                                                     'SENT',
                                                     'SUCCESS',
                                                     'FAILED',
                                                     'COMPENSATING',
                                                     'COMPENSATED',
                                                     'COMPENSATION_FAILED'
                                       )),
                               CONSTRAINT uq_saga_step_log_saga_sequence
                                   UNIQUE (saga_id, sequence),
                               CONSTRAINT uq_saga_step_log_command_id
                                   UNIQUE (command_id)
);

CREATE INDEX idx_saga_step_log_saga_status_sequence
    ON saga_step_log (saga_id, status, sequence);

CREATE INDEX idx_saga_step_log_sent_timeout
    ON saga_step_log (status, timeout_at)
    WHERE status = 'SENT';

CREATE INDEX idx_saga_step_log_pending_stale
    ON saga_step_log (updated_at)
    WHERE status = 'PENDING';

CREATE INDEX idx_saga_instance_active_stale
    ON saga_instance (updated_at)
    WHERE status IN ('STARTED', 'COMPENSATING');