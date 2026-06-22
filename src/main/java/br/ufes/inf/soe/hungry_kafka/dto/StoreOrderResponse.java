package br.ufes.inf.soe.hungry_kafka.dto;

import java.time.Instant;
import java.util.List;

public record StoreOrderResponse(
        Integer id,
        ClientDto client,
        List<OrderItemResponse> items,
        Instant createdAt,
        Instant expectedDelivery,
        String status) {
}
