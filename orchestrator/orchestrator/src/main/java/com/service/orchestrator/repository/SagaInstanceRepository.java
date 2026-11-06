package com.service.orchestrator.repository;

import com.service.orchestrator.entity.SagaInstance;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.UUID;

public interface SagaInstanceRepository extends JpaRepository<SagaInstance, UUID> {
}
