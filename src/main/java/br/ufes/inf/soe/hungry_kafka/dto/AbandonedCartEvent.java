package br.ufes.inf.soe.hungry_kafka.dto;

import java.util.List;

public record AbandonedCartEvent(Integer clientId, List<Integer> productIds) {
}
