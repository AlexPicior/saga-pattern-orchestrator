package com.service.orchestrator.messaging;

import com.service.orchestrator.config.KafkaTopics;
import com.service.orchestrator.dto.SagaReply;
import com.service.orchestrator.service.ReplyHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class StepEventListener {

    public static final String ID = "stepEventListener";

    private static final Logger log = LoggerFactory.getLogger(StepEventListener.class);

    private final ReplyHandler handler;

    public StepEventListener(ReplyHandler handler) {
        this.handler = handler;
    }

    @KafkaListener(id = ID, idIsGroup = false, topics = KafkaTopics.SAGA_REPLIES,
            autoStartup = "${saga.reply-listener.auto-startup:true}")
    public void onReply(SagaReply reply) {
        ReplyHandler.Result result = handler.handle(reply);
        log.debug("{} {} reply for saga {} step {}: {}",
                reply.action(), reply.outcome(), reply.sagaId(), reply.stepId(), result);
    }
}
