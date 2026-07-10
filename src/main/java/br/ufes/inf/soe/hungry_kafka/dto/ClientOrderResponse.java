package br.ufes.inf.soe.hungry_kafka.dto;

import java.time.Instant;
import java.util.List;

public record ClientOrderResponse(
        Integer id,
        Integer clientId,
        List<OrderItemResponse> items,
        Instant createdAt,
        Instant expectedDelivery,
        String status) {
}
