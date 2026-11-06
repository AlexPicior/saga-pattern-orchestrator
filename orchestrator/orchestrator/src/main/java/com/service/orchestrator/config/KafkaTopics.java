package com.service.orchestrator.config;

public final class KafkaTopics {

    public static final String INVENTORY_COMMANDS = "inventory.commands";
    public static final String PAYMENT_COMMANDS = "payment.commands";
    public static final String SHIPPING_COMMANDS = "shipping.commands";
    public static final String SAGA_REPLIES = "saga.replies";
    public static final String SAGA_REPLIES_DLT = SAGA_REPLIES + "-dlt";

    public static final int PARTITIONS = 3;

    private KafkaTopics() {
    }
}
