package com.service.orchestrator.messaging;

import com.service.orchestrator.event.CommandDispatchedEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import org.springframework.transaction.event.TransactionPhase;
import org.springframework.transaction.event.TransactionalEventListener;

@Component
public class CommandDispatchListener {

    private static final Logger log = LoggerFactory.getLogger(CommandDispatchListener.class);

    private final SagaCommandPublisher publisher;

    public CommandDispatchListener(SagaCommandPublisher publisher) {
        this.publisher = publisher;
    }

    @TransactionalEventListener(phase = TransactionPhase.AFTER_COMMIT)
    public void onCommandDispatched(CommandDispatchedEvent event) {
        try {
            publisher.publish(event.topic(), event.command());
        } catch (RuntimeException e) {
            log.warn("Could not publish command {} for saga {} to {}; the timeout poller will resend",
                    event.command().commandId(), event.command().sagaId(), event.topic(), e);
        }
    }
}
