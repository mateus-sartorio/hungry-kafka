package br.ufes.inf.soe.hungry_kafka.dto;

import java.util.List;

public record CreateOrderRequest(Integer clientId, List<OrderItemInput> items) {
    public CreateOrderEvent toEvent() {
        return new CreateOrderEvent(clientId, items);
    }
}
