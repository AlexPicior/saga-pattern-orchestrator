package com.service.orchestrator.service;

import com.service.orchestrator.AbstractPostgresTest;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.UUID;

import static com.service.orchestrator.entity.SagaStatus.*;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class SagaTransitionServiceTest extends AbstractPostgresTest {

    @Autowired
    private SagaTransitionService service;

    @Test
    void transitionChangesStatusAndUpdatedAt() {
        UUID sagaId = seedSaga("TEST", STARTED, 3, "{}");
        SagaRow before = readSaga(sagaId);

        assertThat(service.transition(sagaId, STARTED, COMPLETED)).isTrue();

        SagaRow after = readSaga(sagaId);
        assertThat(after.status()).isEqualTo(COMPLETED);
        assertThat(after.currentStepSequence()).isEqualTo(3);
        assertThat(after.updatedAt()).isAfter(before.updatedAt());
    }

    @Test
    void transitionLosesWhenSagaIsNoLongerInFromStatus() {
        UUID sagaId = seedSaga("TEST", COMPENSATING, 2, "{}");
        SagaRow before = readSaga(sagaId);

        assertThat(service.transition(sagaId, STARTED, COMPLETED)).isFalse();
        assertThat(readSaga(sagaId)).isEqualTo(before);
    }

    @Test
    void invalidSagaEdgeThrowsAndLeavesRowUntouched() {
        UUID sagaId = seedSaga("TEST", STARTED, 1, "{}");
        SagaRow before = readSaga(sagaId);

        assertThatThrownBy(() -> service.transition(sagaId, STARTED, FAILED))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("STARTED -> FAILED");
        assertThat(readSaga(sagaId)).isEqualTo(before);
    }

    @Test
    void advanceStepMovesThePointerByOne() {
        UUID sagaId = seedSaga("TEST", STARTED, 1, "{}");
        SagaRow before = readSaga(sagaId);

        assertThat(service.advanceStep(sagaId, 1)).isTrue();

        SagaRow after = readSaga(sagaId);
        assertThat(after.currentStepSequence()).isEqualTo(2);
        assertThat(after.status()).isEqualTo(STARTED);
        assertThat(after.updatedAt()).isAfter(before.updatedAt());
    }

    @Test
    void advanceStepLosesOnStaleSequence() {
        UUID sagaId = seedSaga("TEST", STARTED, 2, "{}");
        SagaRow before = readSaga(sagaId);

        assertThat(service.advanceStep(sagaId, 1)).isFalse();
        assertThat(readSaga(sagaId)).isEqualTo(before);
    }

    @Test
    void advanceStepNeverMovesASagaThatIsNotStarted() {
        UUID sagaId = seedSaga("TEST", COMPENSATING, 1, "{}");

        assertThat(service.advanceStep(sagaId, 1)).isFalse();
        assertThat(readSaga(sagaId).currentStepSequence()).isEqualTo(1);
    }

    @Test
    void advanceStepRejectsNegativeSequence() {
        UUID sagaId = seedSaga("TEST", STARTED, 0, "{}");

        assertThatThrownBy(() -> service.advanceStep(sagaId, -1)).isInstanceOf(IllegalArgumentException.class);
    }
}
