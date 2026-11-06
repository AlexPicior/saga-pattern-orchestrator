ALTER TABLE saga_step_log
    ADD COLUMN compensation_command_id UUID;

ALTER TABLE saga_step_log
    ADD CONSTRAINT uq_saga_step_log_compensation_command_id UNIQUE (compensation_command_id);
