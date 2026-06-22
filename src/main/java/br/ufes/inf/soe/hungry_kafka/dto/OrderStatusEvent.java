package br.ufes.inf.soe.hungry_kafka.dto;

import java.time.Duration;
import java.time.Instant;

public record OrderStatusEvent(
        String eventId,
        String orderId,
        String userId,
        String storeId,
        String category,
        OrderStatus status,
        Duration expectedDelivery,
        Instant occurredAt) {
}
