package br.ufes.inf.soe.hungry_kafka.dto;

import java.time.Instant;

public record CartEvent(
        String eventId,
        CartAction action,
        Integer currentAmount,
        Integer productId,
        Integer clientId,
        Instant occurredAt) {
}
