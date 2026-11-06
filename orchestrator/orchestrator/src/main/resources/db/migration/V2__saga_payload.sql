ALTER TABLE saga_instance
    ADD COLUMN payload JSONB NOT NULL DEFAULT '{}'::jsonb;
