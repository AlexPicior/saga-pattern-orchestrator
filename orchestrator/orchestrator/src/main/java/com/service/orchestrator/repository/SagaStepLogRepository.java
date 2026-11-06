package com.service.orchestrator.repository;

import com.service.orchestrator.entity.SagaStepLog;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.Optional;
import java.util.UUID;

public interface SagaStepLogRepository extends JpaRepository<SagaStepLog, UUID> {

    Optional<SagaStepLog> findBySagaIdAndSequence(UUID sagaId, int sequence);
}
