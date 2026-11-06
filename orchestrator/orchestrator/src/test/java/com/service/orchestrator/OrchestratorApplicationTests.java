package com.service.orchestrator;

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;

@SpringBootTest(properties = {
		"spring.kafka.admin.auto-create=false",
		"saga.reply-listener.auto-startup=false"})
@Import(TestcontainersConfiguration.class)
class OrchestratorApplicationTests {

	@Test
	void contextLoads() {
	}

}
