package com.service.orchestrator.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.UUID;

@Service
public class OpsAlertService {

    public enum Type {
        REPLY_CONTRADICTS_LOG,
        STEP_NEEDS_ATTENTION,
        COMPENSATION_FAILED,
        CANNOT_COMPENSATE_PIVOT
    }

    private static final Logger log = LoggerFactory.getLogger(OpsAlertService.class);

    public void raise(Type type, UUID sagaId, UUID stepId, String detail) {
        log.error("OPS ALERT {} saga={} step={}: {}", type, sagaId, stepId, detail);
    }
}
