package com.service.orchestrator.dto;

import org.apache.kafka.common.header.internals.RecordHeaders;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.support.serializer.JacksonJsonDeserializer;
import org.springframework.kafka.support.serializer.JacksonJsonSerializer;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class MessageEnvelopeTest {

    private final JsonMapper mapper = JsonMapper.builder().build();

    @Test
    void commandWireFormatIsTheContractServicesRead() {
        UUID sagaId = UUID.randomUUID();
        UUID stepId = UUID.randomUUID();
        UUID commandId = UUID.randomUUID();
        SagaCommand command = new SagaCommand(sagaId, stepId, commandId, StepAction.COMPENSATE, 2,
                Map.of("orderId", "o-1", "amount", 4999));

        RecordHeaders headers = new RecordHeaders();
        byte[] bytes;
        try (var serializer = new JacksonJsonSerializer<SagaCommand>().noTypeInfo()) {
            bytes = serializer.serialize("payment.commands", headers, command);
        }
        JsonNode json = mapper.readTree(bytes);

        assertThat(json.propertyNames())
                .containsExactlyInAnyOrder("sagaId", "stepId", "commandId", "action", "attempt", "payload");
        assertThat(json.get("sagaId").asString()).isEqualTo(sagaId.toString());
        assertThat(json.get("commandId").asString()).isEqualTo(commandId.toString());
        assertThat(json.get("action").asString()).isEqualTo("COMPENSATE");
        assertThat(json.get("attempt").asInt()).isEqualTo(2);
        assertThat(json.get("payload").get("amount").asInt()).isEqualTo(4999);
        assertThat(headers.toArray()).isEmpty();
    }

    @Test
    void replyFromAServiceDeserializesWithoutTypeHeaders() {
        UUID sagaId = UUID.randomUUID();
        UUID stepId = UUID.randomUUID();
        UUID commandId = UUID.randomUUID();
        String json = """
                {"sagaId":"%s","stepId":"%s","commandId":"%s","action":"FORWARD","outcome":"FAILED","reason":"card declined"}
                """.formatted(sagaId, stepId, commandId);

        SagaReply reply = replyDeserializer().deserialize("saga.replies", new RecordHeaders(),
                json.getBytes(StandardCharsets.UTF_8));

        assertThat(reply).isEqualTo(new SagaReply(sagaId, stepId, commandId, StepAction.FORWARD,
                ReplyOutcome.FAILED, "card declined"));
    }

    @Test
    void replyWithoutReasonIsValid() {
        SagaReply reply = new SagaReply(UUID.randomUUID(), UUID.randomUUID(), UUID.randomUUID(),
                StepAction.FORWARD, ReplyOutcome.SUCCESS, null);

        assertThat(reply.reason()).isNull();
    }

    @Test
    void replyWithoutActionIsRejectedAtDeserialization() {
        String json = """
                {"sagaId":"%s","stepId":"%s","commandId":"%s","outcome":"SUCCESS"}
                """.formatted(UUID.randomUUID(), UUID.randomUUID(), UUID.randomUUID());

        assertThatThrownBy(() -> replyDeserializer().deserialize("saga.replies", new RecordHeaders(),
                json.getBytes(StandardCharsets.UTF_8)))
                .hasRootCauseInstanceOf(IllegalArgumentException.class)
                .hasRootCauseMessage("action is required");
    }

    @Test
    void commandRejectsMissingIdsAndNonPositiveAttempt() {
        UUID id = UUID.randomUUID();

        assertThatThrownBy(() -> new SagaCommand(null, id, id, StepAction.FORWARD, 1, Map.of()))
                .isInstanceOf(IllegalArgumentException.class).hasMessage("sagaId is required");
        assertThatThrownBy(() -> new SagaCommand(id, id, null, StepAction.FORWARD, 1, Map.of()))
                .isInstanceOf(IllegalArgumentException.class).hasMessage("commandId is required");
        assertThatThrownBy(() -> new SagaCommand(id, id, id, null, 1, Map.of()))
                .isInstanceOf(IllegalArgumentException.class).hasMessage("action is required");
        assertThatThrownBy(() -> new SagaCommand(id, id, id, StepAction.FORWARD, 0, Map.of()))
                .isInstanceOf(IllegalArgumentException.class).hasMessage("attempt must be >= 1");
    }

    @Test
    void commandPayloadIsDefensivelyCopiedAndNeverNull() {
        UUID id = UUID.randomUUID();
        Map<String, Object> payload = new HashMap<>();
        payload.put("orderId", "o-1");

        SagaCommand command = new SagaCommand(id, id, id, StepAction.FORWARD, 1, payload);
        payload.put("orderId", "changed");

        assertThat(command.payload()).containsEntry("orderId", "o-1");
        assertThatThrownBy(() -> command.payload().put("x", 1)).isInstanceOf(UnsupportedOperationException.class);
        assertThat(new SagaCommand(id, id, id, StepAction.FORWARD, 1, null).payload()).isEmpty();
    }

    private static JacksonJsonDeserializer<SagaReply> replyDeserializer() {
        return new JacksonJsonDeserializer<>(SagaReply.class, false);
    }
}
