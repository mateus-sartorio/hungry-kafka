package br.ufes.inf.soe.hungry_kafka.dto;

import java.util.List;

public record CreateOrderRequest(Integer clientId, List<OrderItemInput> items) {
}
