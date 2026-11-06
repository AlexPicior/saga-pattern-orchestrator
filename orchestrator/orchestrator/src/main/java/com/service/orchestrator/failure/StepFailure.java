package com.service.orchestrator.failure;

import com.service.orchestrator.dto.ReplyOutcome;
import com.service.orchestrator.dto.SagaReply;
import com.service.orchestrator.dto.StepAction;
import org.springframework.util.Assert;

public sealed interface StepFailure {

    record ReplyFailed(SagaReply reply) implements StepFailure {
        public ReplyFailed {
            Assert.notNull(reply, "reply is required");
            Assert.isTrue(reply.outcome() == ReplyOutcome.FAILED, "a " + reply.outcome() + " reply is not a failure");
        }
    }

    record TimedOut(StepAction action, int attempt) implements StepFailure {
        public TimedOut {
            Assert.notNull(action, "action is required");
        }
    }

    record Thrown(Throwable cause) implements StepFailure {
        public Thrown {
            Assert.notNull(cause, "cause is required");
        }
    }
}
